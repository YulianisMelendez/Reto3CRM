import hashlib
import json
import logging
import os
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level="INFO", format="%(asctime)s [SUITECRM] %(message)s")
log = logging.getLogger("suitecrm_mock")

DB_PATH = os.getenv("DB_PATH", "/data/suitecrm.db")
CRM_USER = os.getenv("CRM_USER", "admin")
CRM_PASS = os.getenv("CRM_PASS", "Admin1234")

app = FastAPI(title="SuiteCRM Mock API")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])


# ── Base de datos ─────────────────────────────────────────────────────────────

def get_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS leads (
            id TEXT PRIMARY KEY,
            first_name TEXT DEFAULT '',
            last_name TEXT DEFAULT '',
            company TEXT DEFAULT '',
            email TEXT DEFAULT '',
            status TEXT DEFAULT 'New',
            rating TEXT DEFAULT 'Medium',
            industry TEXT DEFAULT '',
            assigned_user_name TEXT DEFAULT '',
            description TEXT DEFAULT '',
            lead_source TEXT DEFAULT '',
            created_at TEXT,
            updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS calls (
            id TEXT PRIMARY KEY,
            name TEXT,
            status TEXT DEFAULT 'Planned',
            direction TEXT DEFAULT 'Outbound',
            duration_hours TEXT DEFAULT '0',
            duration_minutes TEXT DEFAULT '30',
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS call_leads (
            call_id TEXT,
            lead_id TEXT
        );
        CREATE TABLE IF NOT EXISTS sessions (
            id TEXT PRIMARY KEY,
            user TEXT,
            created_at TEXT
        );
        """)
        conn.commit()


init_db()


# ── REST API v4.1 (compatible con suitecrm_client.py) ────────────────────────

@app.post("/service/v4_1/rest.php")
async def rest_api(
    method: str = Form(...),
    input_type: str = Form("JSON"),
    response_type: str = Form("JSON"),
    rest_data: str = Form("{}"),
):
    try:
        data = json.loads(rest_data)
    except Exception:
        data = {}

    log.info(f"API method={method}")

    if method == "login":
        return _login(data)
    elif method == "get_entry_list":
        return _get_entry_list(data)
    elif method == "set_entry":
        return _set_entry(data)
    elif method == "set_relationship":
        return _set_relationship(data)
    elif method == "get_entries":
        return _get_entries(data)
    else:
        return JSONResponse({"error": f"Unknown method: {method}"})


def _login(data: dict):
    user_auth = data.get("user_auth", {})
    username  = user_auth.get("user_name", "")
    pw_hash   = user_auth.get("password", "")
    expected  = hashlib.md5(CRM_PASS.encode()).hexdigest()

    if username != CRM_USER or pw_hash != expected:
        return JSONResponse({"id": "", "error": "invalid credentials"})

    sid = str(uuid.uuid4())
    with get_db() as conn:
        conn.execute(
            "INSERT INTO sessions VALUES (?,?,?)",
            (sid, username, datetime.now(timezone.utc).isoformat())
        )
        conn.commit()
    log.info(f"Login OK → session {sid[:8]}…")
    return JSONResponse({"id": sid, "module_name": "Users", "name_value_list": {}})


def _get_entry_list(data: dict):
    module = data.get("module_name", "")
    query  = data.get("query", "")
    limit  = data.get("max_results", 20)

    if module == "Leads":
        with get_db() as conn:
            if query and "LIKE" in query.upper():
                try:
                    pattern = query.split("'")[1]
                    rows = conn.execute(
                        "SELECT * FROM leads WHERE description LIKE ? LIMIT ?",
                        (pattern, limit)
                    ).fetchall()
                except Exception:
                    rows = conn.execute("SELECT * FROM leads LIMIT ?", (limit,)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM leads ORDER BY updated_at DESC LIMIT ?",
                                    (limit,)).fetchall()

        entry_list = []
        for r in rows:
            entry_list.append({
                "id": r["id"],
                "module_name": "Leads",
                "name_value_list": {k: {"name": k, "value": r[k]} for k in r.keys()}
            })
        return JSONResponse({"result_count": len(entry_list), "entry_list": entry_list})

    return JSONResponse({"result_count": 0, "entry_list": []})


def _set_entry(data: dict):
    module = data.get("module_name", "")
    nvl    = data.get("name_value_list", {})

    fields = {}
    if isinstance(nvl, list):
        for item in nvl:
            fields[item["name"]] = item["value"]
    elif isinstance(nvl, dict):
        for k, v in nvl.items():
            fields[k] = v.get("value", "") if isinstance(v, dict) else v

    now = datetime.now(timezone.utc).isoformat()
    record_id = fields.get("id") or str(uuid.uuid4())

    with get_db() as conn:
        if module == "Leads":
            conn.execute("""
            INSERT INTO leads (id, first_name, last_name, company, email, status,
                               rating, industry, assigned_user_name, description,
                               lead_source, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
                first_name          = CASE WHEN excluded.first_name          != '' THEN excluded.first_name          ELSE leads.first_name          END,
                last_name           = CASE WHEN excluded.last_name            != '' THEN excluded.last_name            ELSE leads.last_name            END,
                company             = CASE WHEN excluded.company              != '' THEN excluded.company              ELSE leads.company              END,
                email               = CASE WHEN excluded.email                != '' THEN excluded.email                ELSE leads.email                END,
                status              = CASE WHEN excluded.status               != '' THEN excluded.status               ELSE leads.status               END,
                rating              = CASE WHEN excluded.rating               != '' THEN excluded.rating               ELSE leads.rating               END,
                industry            = CASE WHEN excluded.industry             != '' THEN excluded.industry             ELSE leads.industry             END,
                assigned_user_name  = CASE WHEN excluded.assigned_user_name  != '' THEN excluded.assigned_user_name  ELSE leads.assigned_user_name  END,
                description         = CASE WHEN excluded.description          != '' THEN excluded.description          ELSE leads.description          END,
                lead_source         = CASE WHEN excluded.lead_source          != '' THEN excluded.lead_source          ELSE leads.lead_source          END,
                updated_at          = excluded.updated_at
            """, (
                record_id,
                fields.get("first_name", ""),
                fields.get("last_name", ""),
                fields.get("company") or fields.get("account_name", ""),
                fields.get("email1") or fields.get("email", ""),
                fields.get("status", "New"),
                fields.get("rating", "Medium"),
                fields.get("industry", ""),
                fields.get("assigned_user_name", ""),
                fields.get("description", ""),
                fields.get("lead_source", ""),
                now, now
            ))
            conn.commit()
            log.info(f"Lead saved: id={record_id} company={fields.get('company')}")

        elif module == "Calls":
            conn.execute("""
            INSERT INTO calls (id, name, status, direction, duration_hours,
                               duration_minutes, created_at)
            VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name,
                status=excluded.status,
                direction=excluded.direction,
                duration_hours=excluded.duration_hours,
                duration_minutes=excluded.duration_minutes
            """, (
                record_id,
                fields.get("name", "Llamada de seguimiento"),
                fields.get("status", "Planned"),
                fields.get("direction", "Outbound"),
                str(fields.get("duration_hours", "0")),
                str(fields.get("duration_minutes", "30")),
                now
            ))
            conn.commit()
            log.info(f"Call saved: id={record_id} name={fields.get('name')}")

    return JSONResponse({"id": record_id, "entry_list": []})


def _set_relationship(data: dict):
    module1   = data.get("module_name", "")
    module1_id = data.get("module_id", "")
    module2   = data.get("link_field_name", "")
    module2_id = data.get("related_ids", [""])[0] if data.get("related_ids") else ""

    if (module1 == "Calls" or module2 == "calls") and module1_id and module2_id:
        call_id = module1_id if module1 == "Calls" else module2_id
        lead_id = module2_id if module1 == "Calls" else module1_id
        with get_db() as conn:
            conn.execute("INSERT INTO call_leads VALUES (?,?)", (call_id, lead_id))
            conn.commit()
        log.info(f"Relación Calls-Leads creada: call={call_id[:8]} lead={lead_id[:8]}")

    return JSONResponse({"created": 1, "failed": 0, "deleted": 0})


def _get_entries(data: dict):
    return JSONResponse({"entry_list": []})


# ── Healthcheck & Admin ───────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "service": "suitecrm-mock"}


@app.delete("/admin/reset")
def reset_all_data():
    """Borra todos los leads, llamadas y sesiones. Útil para limpiar entre pruebas."""
    with get_db() as conn:
        conn.executescript("""
            DELETE FROM leads;
            DELETE FROM calls;
            DELETE FROM call_leads;
            DELETE FROM sessions;
        """)
        conn.commit()
    log.info("Reset completo: todas las tablas vaciadas")
    return {"ok": True, "message": "Todos los datos eliminados"}


# ── Web UI Estilo SaaS CRM Dribbble ──────────────────────────────────────────


def build_splash_loader(title: str, badge: str, theme_color: str, uid: str = "main") -> str:
    tpl = '''
<!-- CRM SPLASH LOADER OVERLAY -->
<div id="crm-splash-overlay" data-text="__TITLE__" onclick="window.dismissSplash && window.dismissSplash(true)">
  <style>
    #crm-splash-overlay {
      position: fixed;
      inset: 0;
      z-index: 9999999;
      background: radial-gradient(circle at 50% 40%, #111827 0%, #070a11 100%);
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      overflow: hidden;
      cursor: pointer;
      transition: opacity 0.5s cubic-bezier(0.16, 1, 0.3, 1), transform 0.5s cubic-bezier(0.16, 1, 0.3, 1), visibility 0.5s;
    }
    #crm-splash-overlay.fade-out {
      opacity: 0;
      transform: scale(1.02);
      pointer-events: none;
      visibility: hidden;
    }
    .splash-bg-glow {
      position: absolute;
      width: 580px;
      height: 580px;
      border-radius: 50%;
      background: radial-gradient(circle, __THEME__33 0%, __THEME__08 50%, transparent 70%);
      filter: blur(50px);
      pointer-events: none;
      animation: splash-pulse 3s ease-in-out infinite alternate;
    }
    @keyframes splash-pulse {
      0% { transform: scale(0.9); opacity: 0.6; }
      100% { transform: scale(1.15); opacity: 1; }
    }
    .splash-container {
      position: relative;
      z-index: 2;
      display: flex;
      flex-direction: column;
      align-items: center;
      text-align: center;
      padding: 24px;
      max-width: 750px;
      width: 100%;
    }
    .splash-badge {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 6px 18px;
      background: rgba(255, 255, 255, 0.05);
      border: 1px solid rgba(255, 255, 255, 0.12);
      border-radius: 9999px;
      font-size: 0.74rem;
      font-weight: 700;
      color: #c7d2fe;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      margin-bottom: 28px;
      backdrop-filter: blur(10px);
    }
    .splash-badge-dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: __THEME__;
      box-shadow: 0 0 10px __THEME__;
      animation: splash-dot-blink 1.2s ease-in-out infinite;
    }
    @keyframes splash-dot-blink {
      0%, 100% { opacity: 1; transform: scale(1); }
      50% { opacity: 0.4; transform: scale(0.85); }
    }
    .splash-stage {
      position: relative;
      display: inline-block;
      min-height: 80px;
      padding: 10px 24px 20px;
    }
    .splash-writing-line {
      position: relative;
      z-index: 5;
      display: inline-flex;
      align-items: baseline;
      justify-content: flex-start;
      font-family: 'Plus Jakarta Sans', system-ui, -apple-system, sans-serif;
      font-size: clamp(2.8rem, 7vw, 4.4rem);
      font-weight: 800;
      letter-spacing: -0.02em;
      color: #ffffff !important;
      min-width: 260px;
      text-shadow: 0 4px 30px __THEME__88;
    }
    .splash-char {
      display: inline-block;
      color: #ffffff !important;
      opacity: 0;
      transform: translateY(6px) scale(0.92);
      transition: opacity 0.1s ease-out, transform 0.1s ease-out;
      white-space: pre;
    }
    .splash-char.drawn {
      opacity: 1 !important;
      transform: translateY(0) scale(1) !important;
    }
    .splash-pencil {
      position: absolute;
      z-index: 10;
      top: 0;
      left: 0;
      width: 54px;
      height: 54px;
      pointer-events: none;
      transform-origin: 5px 49px;
      filter: drop-shadow(0 8px 18px rgba(0, 0, 0, 0.6));
      transition: transform 0.08s cubic-bezier(0.2, 0.8, 0.4, 1);
      will-change: transform;
    }
    .splash-pencil-svg {
      width: 100%;
      height: 100%;
      overflow: visible;
    }
    .splash-lead-spark {
      position: absolute;
      left: 3px;
      top: 47px;
      width: 7px;
      height: 7px;
      border-radius: 50%;
      background: #ffffff;
      box-shadow: 0 0 12px 4px __THEME__, 0 0 24px 8px __THEME__aa;
      pointer-events: none;
      opacity: 0;
      transform: scale(0.5);
      transition: opacity 0.1s, transform 0.1s;
    }
    .splash-lead-spark.active {
      opacity: 1;
      transform: scale(1.3);
    }
    .splash-stroke-bar {
      position: absolute;
      z-index: 4;
      bottom: 8px;
      left: 24px;
      height: 4px;
      border-radius: 99px;
      background: linear-gradient(90deg, __THEME__, #818cf8, #38bdf8);
      box-shadow: 0 0 16px __THEME__cc;
      width: 0%;
      transition: width 0.08s linear;
    }
  </style>

  <div class="splash-bg-glow"></div>
  <div class="splash-container">
    <div class="splash-badge">
      <span class="splash-badge-dot"></span>
      <span>__BADGE__</span>
    </div>
    
    <div class="splash-stage">
      <div id="splash-writing-line" class="splash-writing-line"></div>
      
      <!-- Lápiz animado con forma idéntica al icono del sistema -->
      <div id="splash-pencil" class="splash-pencil">
        <svg viewBox="0 0 54 54" class="splash-pencil-svg">
          <defs>
            <linearGradient id="pBodyGrad___UID__" x1="0%" y1="0%" x2="100%" y2="100%">
              <stop offset="0%" stop-color="#fbbf24"/>
              <stop offset="100%" stop-color="#f59e0b"/>
            </linearGradient>
            <linearGradient id="pEraserGrad___UID__" x1="0%" y1="0%" x2="100%" y2="100%">
              <stop offset="0%" stop-color="#fb7185"/>
              <stop offset="100%" stop-color="#e11d48"/>
            </linearGradient>
            <filter id="pGlow___UID__" x="-20%" y="-20%" width="140%" height="140%">
              <feDropShadow dx="0" dy="2" stdDeviation="3" flood-color="#000" flood-opacity="0.35"/>
            </filter>
          </defs>
          <g filter="url(#pGlow___UID__)">
            <!-- Borrador redondeado -->
            <path d="M36 8 C38 6 42 6 44 8 C46 10 46 14 44 16 L39 21 L31 13 Z" 
                  fill="url(#pEraserGrad___UID__)" stroke="#0f172a" stroke-width="2.5" stroke-linejoin="round"/>
            <!-- Abrazadera metálica -->
            <path d="M31 13 L39 21 L36 24 L28 16 Z" 
                  fill="#cbd5e1" stroke="#0f172a" stroke-width="2.5" stroke-linejoin="round"/>
            <!-- Cuerpo hexagonal -->
            <path d="M28 16 L36 24 L19 41 L11 33 Z" 
                  fill="url(#pBodyGrad___UID__)" stroke="#0f172a" stroke-width="2.5" stroke-linejoin="round"/>
            <!-- Madera afilada -->
            <path d="M11 33 L19 41 L5 49 Z" 
                  fill="#fef3c7" stroke="#0f172a" stroke-width="2.5" stroke-linejoin="round"/>
            <!-- Punta de grafito en (5, 49) -->
            <path d="M5 49 L9 45 L7 43 Z" 
                  fill="#0f172a" stroke="#0f172a" stroke-width="1"/>
          </g>
        </svg>
        <div id="splash-lead-spark" class="splash-lead-spark"></div>
      </div>
      
      <!-- Trazo inferior que acompaña la escritura -->
      <div id="splash-stroke-bar" class="splash-stroke-bar"></div>
    </div>
  </div>

  <script>
  (function() {
    const overlay = document.getElementById('crm-splash-overlay');
    if (!overlay) return;

    const targetText = overlay.dataset.text || "__TITLE__";
    const line = document.getElementById('splash-writing-line');
    const pencil = document.getElementById('splash-pencil');
    const spark = document.getElementById('splash-lead-spark');
    const strokeBar = document.getElementById('splash-stroke-bar');

    let dismissed = false;
    window.dismissSplash = function(immediate) {
      if (dismissed) return;
      dismissed = true;
      overlay.classList.add('fade-out');
      setTimeout(() => {
        try { overlay.remove(); } catch(e) { overlay.style.display = 'none'; }
      }, immediate ? 150 : 450);
    };

    window.addEventListener('keydown', function(e) {
      if (e.key === 'Escape' || e.key === ' ' || e.key === 'Enter') {
        window.dismissSplash(true);
      }
    }, { once: true });

    line.innerHTML = '';
    const charSpans = [];
    for (let i = 0; i < targetText.length; i++) {
      const sp = document.createElement('span');
      sp.className = 'splash-char';
      sp.textContent = targetText[i];
      line.appendChild(sp);
      charSpans.push(sp);
    }

    function runAnimation() {
      const TIP_X = 5;
      const TIP_Y = 49;

      const stageRect = line.parentElement.getBoundingClientRect();
      const stageLeft = stageRect.left;
      const stageTop = stageRect.top;

      function getTargetPos(index) {
        if (index >= charSpans.length) {
          const last = charSpans[charSpans.length - 1];
          const r = last.getBoundingClientRect();
          return {
            x: (r.right - stageLeft) - TIP_X,
            y: (r.bottom - stageTop - 14) - TIP_Y
          };
        }
        const cur = charSpans[index];
        const r = cur.getBoundingClientRect();
        const posX = (r.left - stageLeft + (r.width * 0.65)) - TIP_X;
        const posY = (r.bottom - stageTop - 14) - TIP_Y;
        return { x: Math.max(0, posX), y: Math.max(0, posY) };
      }

      const p0 = getTargetPos(0);
      pencil.style.transform = `translate(${p0.x - 20}px, ${p0.y - 15}px) rotate(12deg)`;
      pencil.style.opacity = '1';

      let currentIndex = 0;
      const totalChars = charSpans.length;
      const charDelay = 110;

      spark.classList.add('active');

      function drawStep() {
        if (dismissed) return;

        if (currentIndex < totalChars) {
          const pos = getTargetPos(currentIndex);
          const wobble = (currentIndex % 2 === 0 ? -4 : 4);
          pencil.style.transform = `translate(${pos.x}px, ${pos.y}px) rotate(${wobble}deg)`;
          
          charSpans[currentIndex].classList.add('drawn');
          
          const pct = Math.round(((currentIndex + 1) / totalChars) * 100);
          strokeBar.style.width = pct + '%';
          
          currentIndex++;
          setTimeout(drawStep, charDelay);
        } else {
          finishAnimation();
        }
      }

      setTimeout(drawStep, 80);

      function finishAnimation() {
        if (dismissed) return;
        
        spark.classList.remove('active');
        const lastPos = getTargetPos(totalChars);
        pencil.style.transition = 'transform 0.4s cubic-bezier(0.16, 1, 0.3, 1), opacity 0.3s ease';
        pencil.style.transform = `translate(${lastPos.x + 35}px, ${lastPos.y - 35}px) rotate(-15deg)`;
        pencil.style.opacity = '0';

        setTimeout(() => {
          window.dismissSplash(false);
        }, 600);
      }
    }

    setTimeout(runAnimation, 50);
    setTimeout(() => window.dismissSplash(false), 3800);
  })();
  </script>
</div>
'''
    return (
        tpl.replace("__TITLE__", title)
           .replace("__BADGE__", badge)
           .replace("__THEME__", theme_color)
           .replace("__UID__", uid)
    )

_SPLASH_HTML_SUITE = build_splash_loader(
    title="Suit CRM",
    badge="PLATAFORMA MULTI-AGENTE &bull; SUIT CRM",
    theme_color="#5243ea",
    uid="suite"
)


def _avatar_icon(size: int = 38, radius: str = "10px") -> str:
    inner = int(size * 0.55)
    return (
        f'<div style="width:{size}px;height:{size}px;border-radius:{radius};'
        f'background:#eef0f6;display:flex;align-items:center;justify-content:center;flex-shrink:0">'
        f'<svg width="{inner}" height="{inner}" viewBox="0 0 24 24" fill="none">'
        f'<circle cx="12" cy="9" r="4" fill="#9aa1bc"/>'
        f'<path d="M4 20c0-3.3 3.6-6 8-6s8 2.7 8 6" stroke="#9aa1bc" stroke-width="2.2" stroke-linecap="round"/>'
        f'</svg></div>'
    )


@app.get("/actividades", response_class=HTMLResponse)
async def actividades_page():
    with get_db() as conn:
        rows = conn.execute("""
            SELECT c.id, c.name, c.status, c.direction, c.duration_hours, c.duration_minutes,
                   c.created_at, l.company, l.assigned_user_name
            FROM calls c
            LEFT JOIN call_leads cl ON c.id = cl.call_id
            LEFT JOIN leads l ON cl.lead_id = l.id
            ORDER BY c.created_at DESC
            LIMIT 200
        """).fetchall()

    rows_html = ""
    for r in rows:
        duration = f"{r['duration_hours']}h {r['duration_minutes']}m"
        created  = (r['created_at'] or '').replace('T', ' ')[:16]
        company  = r['company'] or '—'
        advisor  = r['assigned_user_name'] or '—'
        status   = r['status'] or 'Planned'
        direction= r['direction'] or 'Outbound'
        st_cls   = "badge st-planned" if status == "Planned" else "badge st-converted"
        rows_html += f"""
        <tr>
          <td style="font-size:0.82rem;font-weight:600">{r['name'] or '—'}</td>
          <td>{company}</td>
          <td>
            <div class="advisor-pill">
              {_avatar_icon(22, "50%")}
              <span>{advisor}</span>
            </div>
          </td>
          <td><span class="{st_cls}">{status}</span></td>
          <td class="text-muted text-xs">{direction}</td>
          <td class="text-muted text-xs">{duration}</td>
          <td class="text-muted text-xs">{created}</td>
        </tr>"""

    empty = '<tr><td colspan="7" style="text-align:center;padding:32px;color:var(--text-muted)">Sin actividades registradas. Procesa un lead para que aparezcan aquí.</td></tr>' if not rows else ""

    return HTMLResponse(f"""<!DOCTYPE html><html lang="es"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Actividades — SuiteCRM</title>
<style>
:root{{--primary:#4f46e5;--primary-light:#eef2ff;--surface:#fff;--bg:#f4f5f9;--border:#e5e7eb;--text-body:#1e1f2e;--text-muted:#9ca3af;--text-secondary:#6b7280;--green:#10b981;--red:#ef4444}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:var(--bg);color:var(--text-body);display:flex;height:100vh}}
.sidebar{{width:220px;background:var(--surface);border-right:1px solid var(--border);display:flex;flex-direction:column;padding:24px 16px;gap:4px;flex-shrink:0}}
.logo{{display:flex;align-items:center;gap:10px;padding:0 8px 20px;border-bottom:1px solid var(--border);margin-bottom:12px}}
.logo-icon{{width:36px;height:36px;background:var(--primary);border-radius:10px;display:flex;align-items:center;justify-content:center;color:#fff;font-weight:800;font-size:16px}}
.logo-text{{font-size:1rem;font-weight:700}}.logo-text span{{color:var(--primary)}}
.nav-item{{display:flex;align-items:center;gap:10px;padding:9px 12px;border-radius:8px;color:var(--text-secondary);text-decoration:none;font-size:0.85rem;font-weight:500;transition:background .15s}}
.nav-item:hover{{background:var(--bg)}}.nav-item.active{{background:var(--primary-light);color:var(--primary);font-weight:600}}
.main{{flex:1;overflow:auto;padding:32px}}
.page-header{{display:flex;align-items:center;justify-content:space-between;margin-bottom:24px}}
.page-title{{font-size:1.4rem;font-weight:700}}
.page-sub{{font-size:0.8rem;color:var(--text-muted);margin-top:2px}}
.card{{background:var(--surface);border-radius:14px;border:1px solid var(--border);overflow:hidden}}
table{{width:100%;border-collapse:collapse}}
th{{padding:10px 16px;text-align:left;font-size:0.72rem;font-weight:600;color:var(--text-muted);letter-spacing:.05em;text-transform:uppercase;border-bottom:1px solid var(--border)}}
td{{padding:12px 16px;border-bottom:1px solid #f3f4f6;font-size:0.83rem;vertical-align:middle}}
tr:last-child td{{border-bottom:none}}
.badge{{display:inline-flex;align-items:center;padding:3px 10px;border-radius:99px;font-size:0.72rem;font-weight:600}}
.st-planned{{background:#fef9c3;color:#854d0e}}.st-converted{{background:#dcfce7;color:#166534}}
.advisor-pill{{display:flex;align-items:center;gap:6px;font-size:0.82rem}}
.text-muted{{color:var(--text-muted)}}.text-xs{{font-size:0.78rem}}
.sidebar-footer{{margin-top:auto;padding:12px 8px;border-top:1px solid var(--border);display:flex;align-items:center;gap:10px}}
.sidebar-footer .user-name{{font-size:0.82rem;font-weight:600}}.sidebar-footer .user-role{{font-size:0.72rem;color:var(--text-muted)}}
</style></head><body>{_SPLASH_HTML_SUITE}
<div class="sidebar">
  <div class="logo">
    <div class="logo-icon">S</div>
    <div class="logo-text"><span>Suite</span>CRM</div>
  </div>
  <div style="display:flex;flex-direction:column;gap:4px">
    <a href="/" class="nav-item">
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/></svg>
      Leads
    </a>
    <a href="/actividades" class="nav-item active">
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="4" width="18" height="18" rx="2" ry="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/></svg>
      Actividades
    </a>
    <a href="http://localhost:8888" target="_blank" class="nav-item">
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect width="7" height="9" x="3" y="3" rx="1"/><rect width="7" height="5" x="14" y="3" rx="1"/><rect width="7" height="9" x="14" y="12" rx="1"/><rect width="7" height="5" x="3" y="16" rx="1"/></svg>
      Operations Hub ↗
    </a>
  </div>
  <div class="sidebar-footer">
    {_avatar_icon(36, "50%")}
    <div>
      <div class="user-name">Administrador</div>
      <div class="user-role">Sistema CRM</div>
    </div>
  </div>
</div>
<div class="main">
  <div class="page-header">
    <div>
      <div class="page-title">Actividades de Seguimiento</div>
      <div class="page-sub">Llamadas y tareas registradas por los agentes · {len(rows)} actividad{'es' if len(rows) != 1 else ''}</div>
    </div>
    <a href="http://localhost:8888" target="_blank" style="font-size:0.82rem;color:var(--primary);text-decoration:none;font-weight:600">Dashboard Operativo ↗</a>
  </div>
  <div class="card">
    <table>
      <thead><tr>
        <th>Actividad</th><th>Empresa</th><th>Asesor</th><th>Estado</th><th>Dirección</th><th>Duración</th><th>Fecha</th>
      </tr></thead>
      <tbody>{rows_html}{empty}</tbody>
    </table>
  </div>
</div>
</body></html>""")


@app.get("/", response_class=HTMLResponse)
@app.get("/index.php", response_class=HTMLResponse)
async def index(request: Request,
                module: Optional[str] = None,
                action: Optional[str] = None,
                record: Optional[str] = None):
    if module == "Leads" and action == "DetailView" and record:
        return _lead_detail_html(record)
    return _leads_list_html()


def _leads_list_html():
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM leads ORDER BY updated_at DESC LIMIT 100"
        ).fetchall()

    rows_html = ""
    for r in rows:
        company_name = r['company'] or r['last_name'] or 'Empresa'
        status = r['status'] or 'New'
        st_cls = "st-" + status.lower().replace(" ", "-")
        rating = r['rating'] or 'Medium'
        rt_cls = "rt-" + rating.lower()
        
        assigned = r['assigned_user_name'] or 'Sin asignar'
        updated = (r['updated_at'] or '').replace('T', ' ')[:16]

        fn = (r['first_name'] or '').strip()
        ln = (r['last_name'] or '').strip()
        _NOMBRES_D   = ["Carlos","María","Andrés","Laura","Daniel","Valentina","Santiago","Camila","Felipe","Natalia","Jorge","Ana","Ricardo","Sofía","Diego"]
        _APELLIDOS_D = ["García","López","Martínez","Rodríguez","Herrera","Torres","Vargas","Castillo","Mendoza","Ruiz","Moreno","Jiménez","Romero","Álvarez","Soto"]
        if fn.lower() == 'lead' or not fn or len(fn) < 3:
            rid = r['id'] or company_name
            h = abs(hash(rid))
            contact_display = f"{_NOMBRES_D[h % len(_NOMBRES_D)]} {_APELLIDOS_D[(h+5) % len(_APELLIDOS_D)]}"
        else:
            contact_display = f"{fn} {ln}".strip()

        rows_html += f"""<tr onclick="location='/index.php?module=Leads&action=DetailView&record={r['id']}'" class="lead-row">
          <td>
            <div class="contact-card-cell">
              {_avatar_icon(38, "10px")}
              <div>
                <div class="contact-name">{contact_display}</div>
                <div class="company-sub">{company_name}</div>
              </div>
            </div>
          </td>
          <td class="text-secondary">{r['email'] or '—'}</td>
          <td><span class="badge {st_cls}">{status}</span></td>
          <td><span class="rating-badge {rt_cls}">{_rating_badge_label(rating)}</span></td>
          <td>
            <div class="advisor-pill">
              {_avatar_icon(22, "50%")}
              <span>{assigned}</span>
            </div>
          </td>
          <td class="text-muted text-xs">{updated}</td>
          <td>
            <button class="action-btn" title="Ver Ficha">
              Ver Ficha
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M5 12h14M12 5l7 7-7 7"/></svg>
            </button>
          </td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SuiteCRM — Plataforma Comercial</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@500;600&display=swap" rel="stylesheet">
<style>
  :root {{
    --primary: #5243ea;
    --primary-light: #f0edff;
    --primary-hover: #4335d6;
    --bg-page: #f4f5fa;
    --card-bg: #ffffff;
    --border-color: #ebedf3;
    --border-light: #f1f3f7;
    --text-heading: #1e2238;
    --text-body: #616886;
    --text-muted: #9aa1bc;
    --green: #10b981;
    --green-light: #ecfdf5;
    --orange: #f59e0b;
    --orange-light: #fffbeb;
    --red: #ef4444;
    --red-light: #fef2f2;
    --blue: #3b82f6;
    --sidebar-w: 240px;
    --shadow-card: 0 4px 20px -2px rgba(30, 34, 56, 0.03), 0 2px 6px -1px rgba(30, 34, 56, 0.02);
    --radius-lg: 20px;
    --radius-md: 14px;
    --radius-full: 9999px;
  }}

  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: 'Plus Jakarta Sans', sans-serif;
    background-color: var(--bg-page);
    color: var(--text-heading);
    min-height: 100vh;
    display: flex;
  }}

  /* ── Sidebar ── */
  aside.sidebar {{
    width: var(--sidebar-w);
    background: #ffffff;
    border-right: 1px solid var(--border-color);
    padding: 24px 18px;
    display: flex;
    flex-direction: column;
    justify-content: space-between;
    position: fixed;
    top: 0;
    bottom: 0;
    left: 0;
    z-index: 100;
  }}

  .sidebar-logo {{
    display: flex;
    align-items: center;
    gap: 12px;
    text-decoration: none;
    padding-left: 8px;
    margin-bottom: 32px;
  }}

  .logo-icon {{
    width: 36px;
    height: 36px;
    border-radius: 12px;
    background: linear-gradient(135deg, #6366f1 0%, #4338ca 100%);
    display: flex;
    align-items: center;
    justify-content: center;
    color: #ffffff;
    box-shadow: 0 4px 12px rgba(99, 102, 241, 0.35);
  }}

  .logo-text {{
    font-size: 1.2rem;
    font-weight: 800;
    color: var(--text-heading);
    letter-spacing: -0.03em;
  }}

  .nav-group {{
    display: flex;
    flex-direction: column;
    gap: 6px;
  }}

  .nav-item {{
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 10px 14px;
    border-radius: var(--radius-md);
    text-decoration: none;
    font-size: 0.86rem;
    font-weight: 600;
    color: var(--text-body);
    transition: all 0.2s ease;
  }}

  .nav-item:hover {{
    color: var(--primary);
    background: var(--primary-light);
  }}

  .nav-item.active {{
    background: var(--primary);
    color: #ffffff;
    box-shadow: 0 6px 16px rgba(82, 67, 234, 0.25);
  }}

  .user-card {{
    background: #f8fafc;
    border: 1px solid var(--border-color);
    border-radius: var(--radius-md);
    padding: 14px;
  }}

  .user-card-top {{
    display: flex;
    align-items: center;
    gap: 10px;
  }}


  .user-info h4 {{
    font-size: 0.82rem;
    font-weight: 700;
    color: var(--text-heading);
  }}

  .user-info p {{
    font-size: 0.7rem;
    color: var(--text-muted);
  }}

  /* ── Main Canvas ── */
  main.main-canvas {{
    margin-left: var(--sidebar-w);
    flex: 1;
    padding: 24px 32px 60px;
    max-width: 1440px;
  }}

  .top-navbar {{
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 24px;
  }}

  .search-box {{
    position: relative;
    width: 380px;
  }}

  .search-box svg {{
    position: absolute;
    left: 14px;
    top: 50%;
    transform: translateY(-50%);
    color: var(--text-muted);
  }}

  .search-box input {{
    width: 100%;
    background: #ffffff;
    border: 1px solid var(--border-color);
    border-radius: var(--radius-full);
    padding: 10px 18px 10px 42px;
    font-size: 0.85rem;
    font-family: inherit;
    color: var(--text-heading);
    box-shadow: var(--shadow-card);
  }}

  .search-box input:focus {{
    outline: none;
    border-color: var(--primary);
  }}

  .btn-dashboard-link {{
    background: #ffffff;
    border: 1px solid var(--border-color);
    color: var(--text-heading);
    padding: 8px 18px;
    border-radius: var(--radius-full);
    font-size: 0.82rem;
    font-weight: 700;
    text-decoration: none;
    display: inline-flex;
    align-items: center;
    gap: 6px;
    box-shadow: var(--shadow-card);
    transition: all 0.2s;
  }}

  .btn-dashboard-link:hover {{
    color: var(--primary);
    border-color: var(--primary);
  }}

  /* Header banner */
  .page-banner {{
    background: #ffffff;
    border: 1px solid var(--border-color);
    border-radius: var(--radius-lg);
    padding: 22px 28px;
    margin-bottom: 24px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    box-shadow: var(--shadow-card);
  }}

  .banner-titles h1 {{
    font-size: 1.4rem;
    font-weight: 800;
    color: var(--text-heading);
    letter-spacing: -0.02em;
  }}

  .banner-subtitle {{
    font-size: 0.82rem;
    color: var(--text-muted);
    margin-top: 4px;
    display: flex;
    align-items: center;
    gap: 6px;
  }}

  .lead-count-badge {{
    background: var(--primary-light);
    color: var(--primary);
    border: 1px solid #ded7fe;
    border-radius: var(--radius-full);
    padding: 5px 16px;
    font-size: 0.82rem;
    font-weight: 700;
  }}

  /* Table card */
  .panel-card {{
    background: #ffffff;
    border: 1px solid var(--border-color);
    border-radius: var(--radius-lg);
    padding: 24px;
    box-shadow: var(--shadow-card);
  }}

  table.data-table {{
    width: 100%;
    border-collapse: separate;
    border-spacing: 0;
    font-size: 0.84rem;
  }}

  table.data-table th {{
    background: #fcfdfe;
    color: var(--text-muted);
    font-size: 0.72rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    padding: 12px 14px;
    text-align: left;
    border-bottom: 1px solid var(--border-color);
  }}

  table.data-table td {{
    padding: 14px;
    border-bottom: 1px solid var(--border-light);
    vertical-align: middle;
    transition: background 0.15s;
  }}

  tr.lead-row {{
    cursor: pointer;
  }}

  tr.lead-row:hover td {{
    background: #fafbfc;
  }}

  .contact-card-cell {{
    display: flex;
    align-items: center;
    gap: 12px;
  }}


  .contact-name {{
    font-weight: 700;
    color: var(--text-heading);
    font-size: 0.88rem;
  }}

  .company-sub {{
    font-size: 0.73rem;
    color: var(--text-muted);
    font-weight: 500;
  }}

  .advisor-pill {{
    display: inline-flex;
    align-items: center;
    gap: 8px;
    background: #f8fafc;
    border: 1px solid var(--border-color);
    padding: 4px 10px 4px 4px;
    border-radius: var(--radius-full);
    font-size: 0.78rem;
    font-weight: 600;
    color: var(--text-heading);
  }}


  .badge {{
    display: inline-flex;
    align-items: center;
    padding: 3px 10px;
    border-radius: var(--radius-full);
    font-size: 0.72rem;
    font-weight: 700;
  }}

  .st-new {{ background: #eff6ff; color: #2563eb; }}
  .st-in-process {{ background: #fff7ed; color: #ea580c; }}
  .st-converted {{ background: #ecfdf5; color: #059669; }}
  .st-prospecting {{ background: #f0fdfa; color: #0d9488; }}

  .rating-badge {{
    display: inline-flex;
    align-items: center;
    padding: 3px 10px;
    border-radius: var(--radius-full);
    font-size: 0.72rem;
    font-weight: 700;
  }}

  .rt-high {{ background: #fef2f2; color: #dc2626; }}
  .rt-medium {{ background: #fffbeb; color: #d97706; }}
  .rt-low {{ background: #f0fdf4; color: #16a34a; }}

  .action-btn {{
    background: #ffffff;
    border: 1px solid var(--border-color);
    border-radius: var(--radius-full);
    padding: 5px 12px;
    font-size: 0.75rem;
    font-weight: 700;
    color: var(--text-heading);
    cursor: pointer;
    display: inline-flex;
    align-items: center;
    gap: 4px;
    transition: all 0.2s;
  }}

  tr.lead-row:hover .action-btn {{
    background: var(--primary);
    color: #ffffff;
    border-color: var(--primary);
  }}
</style>
</head>
<body>
{_SPLASH_HTML_SUITE}

<aside class="sidebar">
  <div>
    <a href="/" class="sidebar-logo">
      <div style="width:32px;height:32px;background:#4338ca;color:#ffffff;border-radius:8px;display:flex;align-items:center;justify-content:center;font-weight:900;font-size:1rem">S</div>
      <div class="logo-text">Suite<strong style="color:#4338ca">CRM</strong></div>
    </a>

    <div class="nav-group">
      <a href="/" class="nav-item active">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/></svg>
        Leads
      </a>
      <a href="/actividades" class="nav-item">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="4" width="18" height="18" rx="2" ry="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/></svg>
        Actividades
      </a>
      <a href="http://localhost:8888" target="_blank" class="nav-item">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect width="7" height="9" x="3" y="3" rx="1"/><rect width="7" height="5" x="14" y="3" rx="1"/><rect width="7" height="9" x="14" y="12" rx="1"/><rect width="7" height="5" x="3" y="16" rx="1"/></svg>
        Operations Hub ↗
      </a>
    </div>
  </div>

  <div class="user-card">
    <div class="user-card-top">
      {_avatar_icon(38, "50%")}
      <div class="user-info">
        <h4>Administrador</h4>
        <p>Sistema CRM</p>
      </div>
    </div>
  </div>
</aside>

<main class="main-canvas">
  <div class="top-navbar">
    <div class="search-box">
      <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.3-4.3"/></svg>
      <input type="text" placeholder="Buscar por contacto o empresa..." oninput="filterTable(this.value)">
    </div>

    <a href="http://localhost:8888" target="_blank" class="btn-dashboard-link">
      <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect width="7" height="9" x="3" y="3" rx="1"/><rect width="7" height="5" x="14" y="3" rx="1"/><rect width="7" height="9" x="14" y="12" rx="1"/><rect width="7" height="5" x="3" y="16" rx="1"/></svg>
      Dashboard Operativo ↗
    </a>
  </div>

  <div class="page-banner">
    <div class="banner-titles">
      <h1>Gestión Comercial de Leads</h1>
    </div>
    <span class="lead-count-badge">{len(rows)} leads registrados</span>
  </div>

  <div class="panel-card">
    <div style="overflow-x:auto">
      <table class="data-table">
        <thead>
          <tr>
            <th>Contacto & Empresa</th>
            <th>Email</th>
            <th>Estado</th>
            <th>Prioridad</th>
            <th>Asesor Comercial</th>
            <th>Actualización</th>
            <th>Acción</th>
          </tr>
        </thead>
        <tbody id="tbody">
          {rows_html or '<tr><td colspan="7" style="text-align:center;color:var(--text-muted);padding:40px">No hay leads registrados aún</td></tr>'}
        </tbody>
      </table>
    </div>
  </div>
</main>

<script>
function filterTable(q) {{
  q = q.toLowerCase();
  document.querySelectorAll('#tbody tr.lead-row').forEach(tr => {{
    tr.style.display = tr.innerText.toLowerCase().includes(q) ? '' : 'none';
  }});
}}
</script>
</body>
</html>"""


def _lead_detail_html(record_id: str):
    with get_db() as conn:
        lead = conn.execute("SELECT * FROM leads WHERE id=?", (record_id,)).fetchone()
        calls_rows = conn.execute(
            "SELECT c.* FROM calls c JOIN call_leads cl ON c.id=cl.call_id WHERE cl.lead_id=?",
            (record_id,)
        ).fetchall()

    if not lead:
        return HTMLResponse("<div style='font-family:sans-serif;text-align:center;padding:60px'><h2 style='color:#dc2626'>Lead no encontrado</h2><br><a href='/' style='color:#5243ea'>← Volver</a></div>")

    fn = (lead['first_name'] or '').strip()
    ln = (lead['last_name'] or '').strip()
    company_name = lead['company'] or ln or 'Empresa'

    _NOMBRES_D   = ["Carlos","María","Andrés","Laura","Daniel","Valentina","Santiago","Camila","Felipe","Natalia","Jorge","Ana","Ricardo","Sofía","Diego"]
    _APELLIDOS_D = ["García","López","Martínez","Rodríguez","Herrera","Torres","Vargas","Castillo","Mendoza","Ruiz","Moreno","Jiménez","Romero","Álvarez","Soto"]
    if fn.lower() == 'lead' or not fn or len(fn) < 3:
        rid = lead['id'] or company_name
        h = abs(hash(rid))
        contact_display = f"{_NOMBRES_D[h % len(_NOMBRES_D)]} {_APELLIDOS_D[(h+5) % len(_APELLIDOS_D)]}"
    else:
        contact_display = f"{fn} {ln}".strip()

    assigned = lead['assigned_user_name'] or 'Carlos Mendez'

    calls_html = ""
    for c in calls_rows:
        calls_html += f"""<tr>
          <td>
            <div style="font-weight:700;color:#1e2238">{c['name']}</div>
            <div style="font-size:0.72rem;color:#9aa1bc">Llamada de prospección comercial</div>
          </td>
          <td><span style="background:#eff6ff;color:#2563eb;padding:3px 10px;border-radius:99px;font-size:0.72rem;font-weight:700">{c['status']}</span></td>
          <td style="color:#616886;font-weight:600">{c['direction']}</td>
          <td style="font-family:monospace;font-size:0.8rem;color:#5243ea">{c['duration_hours']}h {c['duration_minutes']}m</td>
          <td style="font-size:0.75rem;color:#9aa1bc">{(c['created_at'] or '').replace('T',' ')[:16]}</td>
        </tr>"""

    RATING_MAP = {
        "High": ("🔴 Alta", "rt-high"),
        "Medium": ("🟡 Media", "rt-medium"),
        "Low": ("🟢 Baja", "rt-low")
    }
    STATUS_MAP = {
        "New": ("Nuevo", "st-new"),
        "In Process": ("En Proceso", "st-in-process"),
        "Converted": ("Convertido", "st-converted"),
        "Prospecting": ("Prospectando", "st-prospecting"),
    }

    rating_info = RATING_MAP.get(lead['rating'], (lead['rating'] or '—', "rt-medium"))
    status_info = STATUS_MAP.get(lead['status'], (lead['status'] or '—', "st-new"))

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{contact_display} — SuiteCRM</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@500;600&display=swap" rel="stylesheet">
<style>
  :root {{
    --primary: #5243ea;
    --primary-light: #f0edff;
    --bg-page: #f4f5fa;
    --card-bg: #ffffff;
    --border-color: #ebedf3;
    --border-light: #f1f3f7;
    --text-heading: #1e2238;
    --text-body: #616886;
    --text-muted: #9aa1bc;
    --green: #10b981;
    --radius-lg: 20px;
    --radius-md: 14px;
    --radius-full: 9999px;
  }}

  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: 'Plus Jakarta Sans', sans-serif;
    background-color: var(--bg-page);
    color: var(--text-heading);
    min-height: 100vh;
  }}

  header.navbar {{
    background: #ffffff;
    border-bottom: 1px solid var(--border-color);
    padding: 14px 32px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    position: sticky;
    top: 0;
    z-index: 100;
  }}

  .brand-logo {{
    display: flex;
    align-items: center;
    gap: 10px;
    font-weight: 800;
    font-size: 1.15rem;
    color: var(--text-heading);
    text-decoration: none;
  }}

  .brand-icon {{
    width: 32px;
    height: 32px;
    border-radius: 10px;
    background: linear-gradient(135deg, #6366f1 0%, #4338ca 100%);
    display: flex;
    align-items: center;
    justify-content: center;
    color: #ffffff;
  }}

  .container {{
    max-width: 1040px;
    margin: 0 auto;
    padding: 28px 20px 60px;
  }}

  .top-action-bar {{
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 20px;
  }}

  .btn-back {{
    background: #ffffff;
    border: 1px solid var(--border-color);
    color: var(--text-heading);
    font-size: 0.82rem;
    font-weight: 700;
    padding: 8px 16px;
    border-radius: var(--radius-full);
    text-decoration: none;
    display: inline-flex;
    align-items: center;
    gap: 6px;
    transition: all 0.2s;
  }}

  .btn-back:hover {{
    color: var(--primary);
    border-color: var(--primary);
  }}

  .lead-header-card {{
    background: #ffffff;
    border: 1px solid var(--border-color);
    border-radius: var(--radius-lg);
    padding: 24px 28px;
    margin-bottom: 20px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 20px;
    box-shadow: 0 4px 20px -2px rgba(30, 34, 56, 0.03);
  }}

  .lead-title-group {{
    display: flex;
    align-items: center;
    gap: 16px;
  }}


  .lead-title-text h1 {{
    font-size: 1.35rem;
    font-weight: 800;
    color: var(--text-heading);
  }}

  .lead-meta-row {{
    display: flex;
    align-items: center;
    gap: 12px;
    margin-top: 4px;
    font-size: 0.8rem;
    color: var(--text-body);
  }}

  .panel-card {{
    background: #ffffff;
    border: 1px solid var(--border-color);
    border-radius: var(--radius-lg);
    padding: 24px;
    margin-bottom: 20px;
    box-shadow: 0 4px 20px -2px rgba(30, 34, 56, 0.03);
  }}

  .panel-title {{
    font-size: 0.88rem;
    font-weight: 800;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: var(--text-heading);
    margin-bottom: 18px;
    display: flex;
    align-items: center;
    gap: 8px;
  }}

  .fields-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(230px, 1fr));
    gap: 16px;
  }}

  .field-item {{
    background: #fbfbfe;
    border: 1px solid var(--border-color);
    border-radius: var(--radius-md);
    padding: 14px 16px;
  }}

  .field-label {{
    font-size: 0.7rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: var(--text-muted);
    margin-bottom: 6px;
  }}

  .field-value {{
    font-size: 0.94rem;
    font-weight: 700;
    color: var(--text-heading);
  }}

  .advisor-detail-pill {{
    display: inline-flex;
    align-items: center;
    gap: 8px;
    background: #f8fafc;
    border: 1px solid var(--border-color);
    border-radius: var(--radius-full);
    padding: 4px 12px 4px 6px;
    font-weight: 700;
    font-size: 0.82rem;
  }}


  table.detail-table {{
    width: 100%;
    border-collapse: separate;
    border-spacing: 0;
    font-size: 0.84rem;
  }}

  table.detail-table th {{
    background: #fcfdfe;
    color: var(--text-muted);
    font-size: 0.72rem;
    font-weight: 700;
    text-transform: uppercase;
    padding: 10px 14px;
    text-align: left;
    border-bottom: 1px solid var(--border-color);
  }}

  table.detail-table td {{
    padding: 12px 14px;
    border-bottom: 1px solid var(--border-light);
    vertical-align: middle;
  }}
</style>
</head>
<body>
{_SPLASH_HTML_SUITE}

<header class="navbar">
  <a href="/" class="brand-logo">
    <div style="width:28px;height:28px;background:#4338ca;color:#ffffff;border-radius:8px;display:flex;align-items:center;justify-content:center;font-weight:900;font-size:0.95rem">S</div>
    <span>Suite<strong style="color:#4338ca">CRM</strong></span>
  </a>
  <a href="http://localhost:8888" target="_blank" style="text-decoration:none;font-size:0.82rem;font-weight:700;color:var(--primary)">
    Dashboard Operativo ↗
  </a>
</header>

<div class="container">
  <div class="top-action-bar">
    <a href="/" class="btn-back">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M19 12H5M12 19l-7-7 7-7"/></svg>
      Volver al Listado de Leads
    </a>
    <span style="font-size:0.78rem;color:var(--text-muted);font-family:'JetBrains Mono',monospace">ID: {lead['id']}</span>
  </div>

  <div class="lead-header-card">
    <div class="lead-title-group">
      {_avatar_icon(60, "16px")}
      <div class="lead-title-text">
        <h1>{contact_display}</h1>
        <div class="lead-meta-row">
          <span>Empresa: <strong>{company_name}</strong></span>
          <span>&bull;</span>
          <span>Email: {lead['email'] or 'Sin email'}</span>
        </div>
      </div>
    </div>
    <div>
      <span style="background:#eff6ff;color:#2563eb;font-size:0.82rem;padding:6px 16px;border-radius:99px;font-weight:700">{status_info[0]}</span>
    </div>
  </div>

  <div class="panel-card">
    <div class="panel-title">
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="4" width="18" height="18" rx="2" ry="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/></svg>
      Información Comercial del Lead
    </div>
    <div class="fields-grid">
      <div class="field-item">
        <div class="field-label">Empresa / Razón Social</div>
        <div class="field-value">{company_name}</div>
      </div>
      <div class="field-item">
        <div class="field-label">Correo Electrónico</div>
        <div class="field-value" style="color:var(--primary)">{lead['email'] or '—'}</div>
      </div>
      <div class="field-item">
        <div class="field-label">Sector / Industria</div>
        <div class="field-value">{lead['industry'] or 'Tecnología'}</div>
      </div>
      <div class="field-item">
        <div class="field-label">Prioridad Comercial</div>
        <div class="field-value">{rating_info[0]}</div>
      </div>
      <div class="field-item">
        <div class="field-label">Asesor Asignado</div>
        <div class="advisor-detail-pill">
          {_avatar_icon(24, "50%")}
          <span>{assigned}</span>
        </div>
      </div>
      <div class="field-item">
        <div class="field-label">Fuente de Origen</div>
        <div class="field-value">{lead['lead_source'] or 'Campaña Web'}</div>
      </div>
      <div class="field-item" style="grid-column: 1 / -1">
        <div class="field-label">Descripción / Notas Comerciales</div>
        <div class="field-value" style="font-size:0.88rem;font-weight:500;color:#334155;line-height:1.5">{lead['description'] or 'Sin observaciones registradas.'}</div>
      </div>
    </div>
  </div>

  <div class="panel-card">
    <div class="panel-title">
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 16.92v3a2 2 0 0 1-2.18 2 19.79 19.79 0 0 1-8.63-3.07 19.5 19.5 0 0 1-6-6 19.79 19.79 0 0 1-3.07-8.67A2 2 0 0 1 4.11 2h3a2 2 0 0 1 2 1.72 12.84 12.84 0 0 0 .7 2.81 2 2 0 0 1-.45 2.11L8.09 9.91a16 16 0 0 0 6 6l1.27-1.27a2 2 0 0 1 2.11-.45 12.84 12.84 0 0 0 2.81.7A2 2 0 0 1 22 16.92z"/></svg>
      Actividades de Seguimiento ({len(calls_rows)})
    </div>
    {'<table class="detail-table"><thead><tr><th>Nombre de Actividad</th><th>Estado</th><th>Dirección</th><th>Duración Prevista</th><th>Fecha Registro</th></tr></thead><tbody>' + calls_html + '</tbody></table>' if calls_rows else '<div style="color:var(--text-muted);font-size:0.85rem;padding:12px 0">Sin llamadas ni tareas agendadas para este registro.</div>'}
  </div>
</div>

</body>
</html>"""


def _rating_badge_label(rating: str) -> str:
    m = {"High": "🔴 Alta", "Medium": "🟡 Media", "Low": "🟢 Baja"}
    return m.get(rating, rating or "—")
