import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import psycopg2.extras
import redis as redis_lib
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

sys.path.insert(0, "/app/shared")
from messaging import get_redis, get_context, PG_DSN

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s [DASHBOARD] %(levelname)s %(message)s")
log = logging.getLogger("dashboard")

SUITECRM_EXTERNAL_URL = os.getenv("SUITECRM_EXTERNAL_URL", "http://localhost:8081")

app = FastAPI(title="Dashboard Multi-Agente", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

AGENTS = ["coordinator", "analysis", "planning", "executor", "validator", "supervisor"]


def pg_query(sql: str, params=None) -> list:
    try:
        conn = psycopg2.connect(PG_DSN)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or ())
            rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        log.error(f"PG error: {e}")
        return []


@app.get("/health")
def health():
    return {"status": "ok", "service": "dashboard"}


@app.get("/api/heartbeats")
def get_heartbeats():
    """Estado de disponibilidad de todos los agentes via Redis heartbeat."""
    r = get_redis()
    result = {}
    for agent in AGENTS:
        raw = r.get(f"heartbeat:{agent}")
        if raw:
            try:
                data = json.loads(raw)
                result[agent] = {**data, "online": True}
            except Exception:
                result[agent] = {"online": False}
        else:
            result[agent] = {"online": False, "agent": agent}
    return result


@app.get("/api/leads")
def list_leads(limit: int = 50):
    rows = pg_query(
        "SELECT DISTINCT lead_id, MIN(fecha_hora) AS inicio, MAX(fecha_hora) AS ultima_actividad "
        "FROM message_log GROUP BY lead_id ORDER BY ultima_actividad DESC LIMIT %s",
        (limit,)
    )
    r = get_redis()
    result = []
    for row in rows:
        lead_id = row["lead_id"]
        ctx     = get_context(r, lead_id) or {}
        ex_id   = r.get(f"suitecrm_id:{lead_id}")
        suitecrm_id_str = ex_id.decode() if isinstance(ex_id, bytes) else (ex_id if ex_id else None)
        
        datos = ctx.get("datos_lead", {})
        empresa = datos.get("company") or datos.get("empresa") or "-"
        contacto = f"{datos.get('first_name', '')} {datos.get('last_name', '')}".strip() or "-"
        
        result.append({
            "lead_id":          lead_id,
            "estado":           ctx.get("estado", "desconocido"),
            "agente_actual":    ctx.get("agente_actual", "-"),
            "empresa":          empresa,
            "contacto":         contacto,
            "email":            datos.get("email", "-"),
            "inicio":           str(row["inicio"]),
            "ultima_actividad": str(row["ultima_actividad"]),
            "error_codigo":     ctx.get("error_codigo"),
            "suitecrm_id":      suitecrm_id_str,
            "actions_mode":     (ctx.get("resultado_executor") or {}).get("actions_mode", "simulado"),
        })
    return result


@app.get("/api/leads/{lead_id}")
def get_lead_detail(lead_id: str):
    r   = get_redis()
    ctx = get_context(r, lead_id)
    if ctx is None:
        raise HTTPException(404, f"Lead {lead_id} no encontrado")

    mensajes   = pg_query(
        "SELECT * FROM message_log WHERE lead_id=%s ORDER BY fecha_hora", (lead_id,)
    )
    decisiones = pg_query(
        "SELECT * FROM decision_log WHERE lead_id=%s ORDER BY fecha_hora", (lead_id,)
    )
    for m in mensajes:
        m["fecha_hora"] = str(m["fecha_hora"])
    for d in decisiones:
        d["fecha_hora"] = str(d["fecha_hora"])

    ex_id = r.get(f"suitecrm_id:{lead_id}")
    return {
        "lead_id":     lead_id,
        "contexto":    ctx,
        "mensajes":    mensajes,
        "decisiones":  decisiones,
        "pipeline":    _build_pipeline_view(ctx, mensajes),
        "suitecrm_id": ex_id.decode() if isinstance(ex_id, bytes) else (ex_id if ex_id else None),
    }


def _build_pipeline_view(ctx: dict, mensajes: list) -> list:
    agents = ["analysis", "planning", "executor", "validator", "supervisor"]
    pipeline = []
    for ag in agents:
        estado    = "pendiente"
        timestamp = None
        resultado = ctx.get(f"resultado_{ag}")

        for m in mensajes:
            if m.get("agente_destino") == f"agente_{ag}":
                timestamp = m.get("fecha_hora")
                if m.get("resultado") == "dispatched":
                    estado = "en_proceso"
                break

        if resultado:
            estado = "completado"

        modo = None
        if ag == "executor" and resultado:
            modo = resultado.get("actions_mode", "simulado")

        pipeline.append({
            "agente":    ag,
            "estado":    estado,
            "timestamp": timestamp,
            "resultado": resultado,
            "modo_crm":  modo,
        })
    return pipeline


@app.get("/api/stats")
def get_stats():
    total    = pg_query("SELECT COUNT(DISTINCT lead_id) AS cnt FROM message_log")
    msgs_hoy = pg_query(
        "SELECT COUNT(*) AS cnt FROM message_log WHERE fecha_hora >= CURRENT_DATE"
    )
    r = get_redis()
    keys = r.keys("lead:context:*")
    state_counts: dict = {}
    for k in keys:
        raw = r.get(k)
        if raw:
            try:
                ctx = json.loads(raw)
                s   = ctx.get("estado", "desconocido")
                state_counts[s] = state_counts.get(s, 0) + 1
            except Exception:
                pass
    return {
        "leads_totales":  total[0]["cnt"] if total else 0,
        "leads_en_redis": len(keys),
        "por_estado":     state_counts,
        "mensajes_hoy":   msgs_hoy[0]["cnt"] if msgs_hoy else 0,
        "timestamp":      datetime.now(timezone.utc).isoformat(),
    }




# ── UI HTML 100% Reto 3 CRM Real ───────────────────────────────────────────────

_HTML = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Reto 3 CRM — Monitor de Pipeline Multi-Agente</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<style>
  :root {
    --primary: #4338ca;
    --primary-light: #eef2ff;
    --primary-hover: #3730a3;
    --accent: #0284c7;
    --bg-page: #f8fafc;
    --card-bg: #ffffff;
    --border: #e2e8f0;
    --border-light: #f1f5f9;
    --text-heading: #0f172a;
    --text-body: #475569;
    --text-muted: #94a3b8;
    --green: #10b981;
    --green-light: #ecfdf5;
    --amber: #f59e0b;
    --amber-light: #fffbeb;
    --red: #ef4444;
    --red-light: #fef2f2;
    --sidebar-w: 250px;
    --radius: 12px;
    --radius-full: 9999px;
  }

  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: 'Plus Jakarta Sans', sans-serif;
    background-color: var(--bg-page);
    color: var(--text-heading);
    min-height: 100vh;
    display: flex;
  }

  /* ── Sidebar ── */
  aside.sidebar {
    width: var(--sidebar-w);
    background: #ffffff;
    border-right: 1px solid var(--border);
    padding: 24px 18px;
    display: flex;
    flex-direction: column;
    justify-content: space-between;
    position: fixed;
    top: 0;
    bottom: 0;
    left: 0;
    z-index: 100;
  }

  .sidebar-header {
    display: flex;
    align-items: center;
    gap: 12px;
    text-decoration: none;
    padding: 0 6px 24px;
    border-bottom: 1px solid var(--border-light);
    margin-bottom: 20px;
  }

  .brand-tag {
    font-size: 1.15rem;
    font-weight: 800;
    color: var(--text-heading);
    letter-spacing: -0.02em;
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .brand-badge-circle {
    width: 10px;
    height: 10px;
    border-radius: 50%;
    background: var(--primary);
    box-shadow: 0 0 0 3px rgba(67, 56, 202, 0.2);
  }

  .nav-list {
    display: flex;
    flex-direction: column;
    gap: 6px;
  }

  .nav-link {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 10px 14px;
    border-radius: var(--radius);
    text-decoration: none;
    font-size: 0.85rem;
    font-weight: 600;
    color: var(--text-body);
    transition: all 0.15s ease;
    cursor: pointer;
  }

  .nav-link:hover {
    background: var(--primary-light);
    color: var(--primary);
  }

  .nav-link.active {
    background: var(--primary);
    color: #ffffff;
  }

  .nav-link.active svg {
    color: #ffffff;
  }

  .sidebar-footer-box {
    background: #f8fafc;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 14px;
    font-size: 0.75rem;
  }

  .sidebar-footer-box h5 {
    font-size: 0.8rem;
    font-weight: 700;
    color: var(--text-heading);
    margin-bottom: 4px;
  }

  .sidebar-footer-box p {
    color: var(--text-muted);
    line-height: 1.4;
  }

  /* ── Main Canvas ── */
  main.main-canvas {
    margin-left: var(--sidebar-w);
    flex: 1;
    padding: 24px 32px 60px;
    max-width: 1400px;
  }

  /* ── Topbar ── */
  .top-navbar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 24px;
  }

  .page-title h1 {
    font-size: 1.35rem;
    font-weight: 800;
    color: var(--text-heading);
  }

  .page-title p {
    font-size: 0.8rem;
    color: var(--text-muted);
    margin-top: 2px;
  }

  .top-actions {
    display: flex;
    align-items: center;
    gap: 12px;
  }

  .btn-primary {
    background: var(--primary);
    color: #ffffff;
    border: none;
    border-radius: var(--radius);
    padding: 9px 18px;
    font-size: 0.82rem;
    font-weight: 700;
    cursor: pointer;
    display: inline-flex;
    align-items: center;
    gap: 8px;
    text-decoration: none;
    transition: all 0.2s;
  }

  .btn-primary:hover {
    background: var(--primary-hover);
  }

  .btn-secondary {
    background: #ffffff;
    border: 1px solid var(--border);
    color: var(--text-heading);
    border-radius: var(--radius);
    padding: 9px 16px;
    font-size: 0.82rem;
    font-weight: 700;
    cursor: pointer;
    display: inline-flex;
    align-items: center;
    gap: 8px;
    text-decoration: none;
  }

  .btn-secondary:hover {
    border-color: var(--primary);
    color: var(--primary);
  }

  /* ── 4 KPI Cards (100% Real) ── */
  .kpi-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 16px;
    margin-bottom: 24px;
  }

  .kpi-card {
    background: var(--card-bg);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 18px 20px;
  }

  .kpi-label {
    font-size: 0.72rem;
    font-weight: 700;
    color: var(--text-muted);
    text-transform: uppercase;
    letter-spacing: 0.04em;
    margin-bottom: 8px;
  }

  .kpi-val {
    font-size: 1.8rem;
    font-weight: 800;
    color: var(--text-heading);
    line-height: 1;
    margin-bottom: 4px;
  }

  .kpi-desc {
    font-size: 0.75rem;
    color: var(--text-body);
  }

  /* ── Architecture Pipeline Flow Card ── */
  .arch-card {
    background: var(--card-bg);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 22px;
    margin-bottom: 24px;
  }

  .arch-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 18px;
  }

  .arch-header h3 {
    font-size: 0.95rem;
    font-weight: 800;
    color: var(--text-heading);
  }

  .agents-flow-grid {
    display: grid;
    grid-template-columns: repeat(6, 1fr);
    gap: 12px;
  }

  @media(max-width: 1024px) {
    .agents-flow-grid { grid-template-columns: repeat(3, 1fr); }
  }

  @media(max-width: 640px) {
    .agents-flow-grid { grid-template-columns: 1fr; }
  }

  .agent-box {
    background: #fbfbfe;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 14px;
    text-align: left;
    transition: all 0.2s;
  }

  .agent-box:hover {
    border-color: var(--primary);
    background: #ffffff;
    box-shadow: 0 4px 12px rgba(67, 56, 202, 0.08);
  }

  .agent-box-top {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 6px;
  }

  .agent-title {
    font-size: 0.85rem;
    font-weight: 700;
    color: var(--text-heading);
    text-transform: capitalize;
  }

  .status-dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: var(--green);
  }

  .status-dot.off {
    background: var(--red);
  }

  .agent-sub {
    font-size: 0.72rem;
    color: var(--text-muted);
    line-height: 1.3;
    margin-bottom: 8px;
  }

  .agent-ping {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.68rem;
    color: var(--text-body);
    font-weight: 600;
  }

  /* ── Leads Table ── */
  .table-card {
    background: var(--card-bg);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 22px;
  }

  .table-toolbar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 18px;
    flex-wrap: wrap;
    gap: 12px;
  }

  .search-input {
    background: #ffffff;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 8px 14px;
    font-size: 0.82rem;
    width: 280px;
    font-family: inherit;
  }

  .search-input:focus {
    outline: none;
    border-color: var(--primary);
  }

  .tabs-group {
    display: flex;
    gap: 4px;
    background: #f1f5f9;
    padding: 3px;
    border-radius: var(--radius);
  }

  .tab-btn {
    background: transparent;
    border: none;
    padding: 6px 12px;
    border-radius: 8px;
    font-size: 0.76rem;
    font-weight: 700;
    color: var(--text-body);
    cursor: pointer;
  }

  .tab-btn.active {
    background: #ffffff;
    color: var(--primary);
  }

  table.data-table {
    width: 100%;
    border-collapse: separate;
    border-spacing: 0;
    font-size: 0.83rem;
  }

  table.data-table th {
    background: #f8fafc;
    color: var(--text-muted);
    font-size: 0.72rem;
    font-weight: 700;
    text-transform: uppercase;
    padding: 10px 14px;
    text-align: left;
    border-bottom: 1px solid var(--border);
  }

  table.data-table td {
    padding: 13px 14px;
    border-bottom: 1px solid var(--border-light);
    vertical-align: middle;
  }

  tr.lead-row:hover td {
    background: #fafbfc;
  }

  .lead-id-text {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.75rem;
    font-weight: 600;
    color: var(--text-body);
  }

  .company-title {
    font-weight: 700;
    color: var(--text-heading);
  }

  .company-contact {
    font-size: 0.72rem;
    color: var(--text-muted);
  }

  .badge-status {
    display: inline-flex;
    align-items: center;
    padding: 3px 9px;
    border-radius: var(--radius-full);
    font-size: 0.72rem;
    font-weight: 700;
    text-transform: capitalize;
  }

  .st-completado { background: var(--green-light); color: var(--green); }
  .st-en_proceso { background: var(--amber-light); color: var(--amber); }
  .st-fallido { background: var(--red-light); color: var(--red); }

  .crm-link {
    color: var(--primary);
    font-weight: 700;
    text-decoration: none;
    font-size: 0.78rem;
  }

  /* ── Drawer Inspector ── */
  #detail-drawer {
    display: none;
    margin-top: 24px;
    background: #ffffff;
    border: 2px solid var(--primary-light);
    border-radius: var(--radius);
    padding: 24px;
  }

  .drawer-title-bar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding-bottom: 14px;
    border-bottom: 1px solid var(--border);
    margin-bottom: 18px;
  }

  .pipeline-visual {
    display: flex;
    align-items: center;
    justify-content: space-between;
    background: #f8fafc;
    border-radius: var(--radius);
    padding: 16px;
    margin-bottom: 20px;
  }

  .pipe-step {
    text-align: center;
    flex: 1;
  }

  .pipe-dot {
    width: 32px;
    height: 32px;
    border-radius: 50%;
    display: flex;
    align-items: center;
    justify-content: center;
    margin: 0 auto 6px;
    font-size: 0.78rem;
    font-weight: 800;
  }

  .pipe-dot.done { background: var(--green); color: #ffffff; }
  .pipe-dot.curr { background: var(--primary); color: #ffffff; }
  .pipe-dot.pend { background: #cbd5e1; color: #ffffff; }

  pre.json-code {
    background: #0f172a;
    color: #f8fafc;
    border-radius: var(--radius);
    padding: 14px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.76rem;
    max-height: 240px;
    overflow: auto;
  }

</style>
</head>
<body>

<!-- Sidebar Navigation -->
<aside class="sidebar">
  <div>
    <div class="sidebar-header">
      <div class="brand-tag">
        <span class="brand-badge-circle"></span>
        <span>Reto 3 CRM</span>
      </div>
    </div>

    <div class="nav-list">
      <a href="#monitor-section" class="nav-link active">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/></svg>
        Monitor de Pipeline
      </a>
      <a href="#arch-section" class="nav-link">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-2 2 2 2 0 0 1-2-2v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1-2-2 2 2 0 0 1 2-2h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 2-2 2 2 0 0 1 2 2v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 2 2 2 2 0 0 1-2 2h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>
        Agentes Autónomos
      </a>
      <a href="__CRM_URL__" target="_blank" class="nav-link">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6M15 3h6v6M10 14L21 3"/></svg>
        SuiteCRM Real ↗
      </a>
    </div>
  </div>
</aside>

<!-- Main Canvas -->
<main class="main-canvas" id="monitor-section">

  <!-- Top Navbar -->
  <div class="top-navbar">
    <div class="page-title">
      <h1>Panel de Operaciones & Trazabilidad</h1>
      <p>Gestión distribuida de leads y sincronización en tiempo real con SuiteCRM</p>
    </div>

    <div class="top-actions">
      <button class="btn-secondary" onclick="refresh()">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21.5 2v6h-6M21.34 15.57a10 10 0 1 1-.57-8.38l5.67-5.67"/></svg>
        Refrescar
      </button>
      <a href="__CRM_URL__" target="_blank" class="btn-primary">
        Ver en SuiteCRM ↗
      </a>
    </div>
  </div>

  <!-- 4 KPIs Reales del Sistema -->
  <div class="kpi-grid">
    <div class="kpi-card">
      <div class="kpi-label">Leads Totales</div>
      <div class="kpi-val" id="stat-total">&ndash;</div>
      <div class="kpi-desc">Registrados en PostgreSQL</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Sincronizados en SuiteCRM</div>
      <div class="kpi-val" id="stat-crm">&ndash;</div>
      <div class="kpi-desc">Con ID externo verificado</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Memoria Activa</div>
      <div class="kpi-val" id="stat-redis">&ndash;</div>
      <div class="kpi-desc">Contextos en Redis</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-label">Transacciones Hoy</div>
      <div class="kpi-val" id="stat-hoy">&ndash;</div>
      <div class="kpi-desc">Eventos en RabbitMQ</div>
    </div>
  </div>

  <!-- Diagrama de Flujo Real de Agentes -->
  <div class="arch-card" id="arch-section">
    <div class="arch-header">
      <div>
        <h3>Arquitectura Multi-Agente & Heartbeats</h3>
        <p style="font-size:0.75rem;color:var(--text-muted);margin-top:2px">Topología centralizada con broker RabbitMQ y estado en Redis</p>
      </div>
      <span style="font-size:0.75rem;color:var(--green);font-weight:700">● Cluster en línea</span>
    </div>

    <div class="agents-flow-grid" id="agents-grid">
      <!-- Inyectado dinámicamente -->
    </div>
  </div>

  <!-- Tabla de Leads en Vivo -->
  <div class="table-card">
    <div class="table-toolbar">
      <div>
        <h3 style="font-size:0.95rem;font-weight:800">Leads Procesados por el Pipeline</h3>
        <p style="font-size:0.75rem;color:var(--text-muted);margin-top:2px">Haz clic en "Ver Trazabilidad" para inspeccionar el flujo</p>
      </div>

      <div style="display:flex;align-items:center;gap:12px">
        <div class="tabs-group">
          <button class="tab-btn active" onclick="filterTable('todos', this)">Todos</button>
          <button class="tab-btn" onclick="filterTable('completado', this)">Completados</button>
          <button class="tab-btn" onclick="filterTable('en_proceso', this)">En Proceso</button>
        </div>

        <input type="text" class="search-input" placeholder="Buscar por ID o Empresa..." oninput="searchTable(this.value)">
      </div>
    </div>

    <div style="overflow-x:auto">
      <table class="data-table">
        <thead>
          <tr>
            <th>Lead ID</th>
            <th>Empresa & Contacto</th>
            <th>Estado Pipeline</th>
            <th>Agente Actual</th>
            <th>Modo CRM</th>
            <th>SuiteCRM</th>
            <th>Última Actividad</th>
            <th>Acción</th>
          </tr>
        </thead>
        <tbody id="leads-tbody">
          <!-- Inyectado -->
        </tbody>
      </table>
    </div>
  </div>

  <!-- Drawer / Inspector de Lead -->
  <div id="detail-drawer">
    <div class="drawer-title-bar">
      <div>
        <h3 style="font-size:1.05rem;font-weight:800" id="drawer-title">Trazabilidad del Lead</h3>
        <span style="font-family:'JetBrains Mono';font-size:0.75rem;color:var(--text-muted)" id="drawer-id"></span>
      </div>
      <div id="drawer-btn-crm"></div>
    </div>

    <div class="pipeline-visual" id="drawer-pipeline"></div>

    <div style="display:grid;grid-template-columns:1fr 1fr;gap:18px">
      <div>
        <h4 style="font-size:0.8rem;color:var(--text-muted);font-weight:700;margin-bottom:8px">CONTEXTO EN REDIS</h4>
        <pre class="json-code" id="drawer-json"></pre>
      </div>
      <div>
        <h4 style="font-size:0.8rem;color:var(--text-muted);font-weight:700;margin-bottom:8px">MENSAJES EN RABBITMQ / POSTGRES</h4>
        <div style="background:#f8fafc;border:1px solid var(--border);border-radius:12px;padding:12px;max-height:240px;overflow-y:auto" id="drawer-messages"></div>
      </div>
    </div>
  </div>

</main>

<script>
const CRM_BASE = "__CRM_URL__";
let currentLeads = [];
let currentFilter = 'todos';

const AGENT_ROLES = {
  coordinator: "Enrutamiento centralizado y RabbitMQ",
  analysis: "Clasificación y cálculo de prioridad",
  planning: "Asignación de asesor y tareas",
  executor: "Integración REST v4.1 con SuiteCRM",
  validator: "Validación de políticas y reglas",
  supervisor: "Control de SLAs y excepciones"
};

async function loadStats() {
  try {
    const s = await fetch('/api/stats').then(r=>r.json());
    document.getElementById('stat-total').textContent = s.leads_totales || 0;
    document.getElementById('stat-redis').textContent = s.leads_en_redis || 0;
    document.getElementById('stat-hoy').textContent = s.mensajes_hoy || 0;
    
    // Contar leads sincronizados con suitecrm
    let crmCount = currentLeads.filter(l => l.suitecrm_id).length;
    document.getElementById('stat-crm').textContent = crmCount;
  } catch(e) {}
}

async function loadHeartbeats() {
  try {
    const data = await fetch('/api/heartbeats').then(r=>r.json());
    document.getElementById('agents-grid').innerHTML = Object.entries(data).map(([name, info]) => {
      const on = info.online;
      const ts = info.timestamp ? info.timestamp.slice(11,19) : 'Sin señal';
      const role = AGENT_ROLES[name] || 'Agente';
      return `<div class="agent-box">
        <div class="agent-box-top">
          <span class="agent-title">${name}</span>
          <span class="status-dot ${on ? '' : 'off'}" title="${on ? 'Online' : 'Offline'}"></span>
        </div>
        <div class="agent-sub">${role}</div>
        <div class="agent-ping">${on ? 'Ping ' + ts : 'Desconectado'}</div>
      </div>`;
    }).join('');
  } catch(e) {}
}

async function loadLeads() {
  try {
    const data = await fetch('/api/leads').then(r=>r.json());
    currentLeads = data || [];
    renderLeads();
    loadStats();
  } catch(e) {}
}

function renderLeads() {
  const tbody = document.getElementById('leads-tbody');
  let filtered = currentLeads;
  if (currentFilter !== 'todos') {
    filtered = currentLeads.filter(l => (l.estado||'').toLowerCase().includes(currentFilter));
  }

  if (!filtered.length) {
    tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;color:var(--text-muted);padding:36px">No hay leads registrados aún</td></tr>';
    return;
  }

  tbody.innerHTML = filtered.map(l => {
    const stCls = (l.estado === 'completado') ? 'st-completado' : ((l.estado||'').includes('error') ? 'st-fallido' : 'st-en_proceso');
    const crmCell = l.suitecrm_id
      ? `<a class="crm-link" href="${CRM_BASE}/index.php?module=Leads&action=DetailView&record=${l.suitecrm_id}" target="_blank">Ver en SuiteCRM ↗</a>`
      : '<span style="color:var(--text-muted);font-size:0.75rem">&mdash;</span>';

    return `<tr class="lead-row">
      <td><span class="lead-id-text">${l.lead_id}</span></td>
      <td>
        <div class="company-title">${l.empresa || '-'}</div>
        <div class="company-contact">${l.contacto !== '-' ? l.contacto : l.email || ''}</div>
      </td>
      <td><span class="badge-status ${stCls}">${l.estado}</span></td>
      <td><span style="font-weight:600;font-size:0.78rem">${l.agente_actual || '-'}</span></td>
      <td><span style="font-size:0.73rem;background:#f1f5f9;padding:2px 8px;border-radius:99px">${l.actions_mode}</span></td>
      <td>${crmCell}</td>
      <td style="font-size:0.75rem;color:var(--text-muted)">${(l.ultima_actividad||'').replace('T',' ').slice(0,16)}</td>
      <td>
        <button class="btn-secondary" style="padding:4px 10px;font-size:0.74rem" onclick="inspectLead('${l.lead_id}')">
          Ver Trazabilidad &rarr;
        </button>
      </td>
    </tr>`;
  }).join('');
}

function filterTable(filter, el) {
  currentFilter = filter;
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  if (el) el.classList.add('active');
  renderLeads();
}

function searchTable(q) {
  q = q.toLowerCase();
  document.querySelectorAll('#leads-tbody tr.lead-row').forEach(tr => {
    tr.style.display = tr.innerText.toLowerCase().includes(q) ? '' : 'none';
  });
}

async function inspectLead(leadId) {
  const drawer = document.getElementById('detail-drawer');
  drawer.style.display = 'block';
  document.getElementById('drawer-id').textContent = 'LEAD ID: ' + leadId;

  try {
    const d = await fetch('/api/leads/' + leadId).then(r=>r.json());
    document.getElementById('drawer-title').textContent = 'Trazabilidad: ' + (d.contexto?.datos_lead?.company || leadId);
    document.getElementById('drawer-json').textContent = JSON.stringify(d.contexto, null, 2);

    document.getElementById('drawer-btn-crm').innerHTML = d.suitecrm_id
      ? `<a href="${CRM_BASE}/index.php?module=Leads&action=DetailView&record=${d.suitecrm_id}" target="_blank" class="btn-primary" style="padding:6px 14px;font-size:0.78rem">Abrir Ficha en SuiteCRM ↗</a>`
      : '';

    // Pipeline steps
    const steps = d.pipeline || [];
    document.getElementById('drawer-pipeline').innerHTML = steps.map(s => {
      const isDone = s.estado === 'completado';
      const isCurr = s.estado === 'en_proceso';
      const cls = isDone ? 'done' : (isCurr ? 'curr' : 'pend');
      const icon = isDone ? '✓' : (isCurr ? '●' : '○');
      return `<div class="pipe-step">
        <div class="pipe-dot ${cls}">${icon}</div>
        <div style="font-size:0.75rem;font-weight:700;text-transform:capitalize">${s.agente}</div>
        <div style="font-size:0.68rem;color:var(--text-muted)">${s.estado}</div>
      </div>`;
    }).join('');

    // Messages
    document.getElementById('drawer-messages').innerHTML = (d.mensajes || []).map(m => `
      <div style="font-size:0.73rem;padding:6px 0;border-bottom:1px solid #e2e8f0;display:flex;justify-content:space-between">
        <div><strong>${m.origen}</strong> &rarr; <span style="color:var(--primary);font-weight:600">${m.agente_destino}</span>: ${m.tipo_evento}</div>
        <span style="color:var(--text-muted);font-family:'JetBrains Mono'">${(m.fecha_hora||'').slice(11,19)}</span>
      </div>
    `).join('') || '<div style="color:var(--text-muted);font-size:0.75rem">Sin eventos</div>';

    drawer.scrollIntoView({ behavior: 'smooth' });
  } catch(e) {}
}

function refresh() {
  loadLeads();
  loadHeartbeats();
}

refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard_ui():
    html = _HTML.replace("__CRM_URL__", SUITECRM_EXTERNAL_URL)
    return html
