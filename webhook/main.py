import json
import logging
import os
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import pika
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# Agregar shared al path
import sys
sys.path.insert(0, "/app/shared")
from messaging import (
    get_rabbitmq_connection, declare_topology, build_message,
    publish, log_message, get_redis, save_context, get_context
)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s [%(name)s] %(levelname)s %(message)s")
log = logging.getLogger("webhook")

# ── Estado global de conexión RabbitMQ ────────────────────────────────────────
rabbit_conn: Optional[pika.BlockingConnection] = None
rabbit_channel = None


def get_channel():
    global rabbit_conn, rabbit_channel
    try:
        if rabbit_conn is None or rabbit_conn.is_closed:
            rabbit_conn = get_rabbitmq_connection()
            rabbit_channel = rabbit_conn.channel()
            declare_topology(rabbit_channel)
        elif rabbit_channel is None or rabbit_channel.is_closed:
            rabbit_channel = rabbit_conn.channel()
            declare_topology(rabbit_channel)
    except Exception as e:
        log.error(f"Error reconectando RabbitMQ: {e}")
        rabbit_conn, rabbit_channel = None, None
        raise
    return rabbit_channel


_hb_stop = threading.Event()

def _heartbeat_loop():
    r = get_redis()
    while not _hb_stop.is_set():
        try:
            r.set("heartbeat:coordinator", json.dumps({
                "agent": "coordinator", "status": "online",
                "ts": datetime.now(timezone.utc).isoformat(),
            }), ex=90)
        except Exception:
            pass
        _hb_stop.wait(30)


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Iniciando Webhook API...")
    try:
        get_channel()
        log.info("RabbitMQ listo")
    except Exception as e:
        log.warning(f"No se pudo conectar al inicio: {e}")
    _hb_stop.clear()
    threading.Thread(target=_heartbeat_loop, daemon=True).start()
    yield
    _hb_stop.set()
    if rabbit_conn and not rabbit_conn.is_closed:
        rabbit_conn.close()
    log.info("Webhook API detenido")


app = FastAPI(
    title="Plataforma Multi-Agente — Webhook API",
    description="Recibe eventos de SuiteCRM y los enruta al Agente Coordinador",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Modelos Pydantic ──────────────────────────────────────────────────────────

class LeadData(BaseModel):
    id:             str = Field(..., description="ID del Lead en SuiteCRM")
    first_name:     Optional[str] = None
    last_name:      Optional[str] = None
    company:        Optional[str] = None
    email:          Optional[str] = None
    phone:          Optional[str] = None
    sector:         Optional[str] = None
    annual_revenue: Optional[float] = None
    description:    Optional[str] = None
    status:         Optional[str] = "New"
    assigned_user:  Optional[str] = None
    extra:          Optional[dict] = {}


class WebhookEvent(BaseModel):
    event_type: str = Field(..., description="lead.created | lead.updated | lead.converted")
    lead:       LeadData
    timestamp:  Optional[str] = None
    source:     Optional[str] = "suitecrm"


class DirectLeadPayload(BaseModel):
    lead_id:        str
    company:        Optional[str] = "Empresa Test"
    sector:         Optional[str] = None
    annual_revenue: Optional[float] = None
    email:          Optional[str] = None
    phone:          Optional[str] = None
    description:    Optional[str] = None
    assigned_user:  Optional[str] = None


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "service": "webhook", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/docs-info")
def docs_info():
    return {"status": "ok", "docs": "/docs", "openapi": "/openapi.json"}


@app.post("/webhook/suitecrm", status_code=202)
async def receive_suitecrm_event(event: WebhookEvent, bg: BackgroundTasks):
    """Recibe un evento de SuiteCRM y lo envía al coordinador."""
    lead_id = event.lead.id
    log.info(f"Evento recibido: {event.event_type} lead={lead_id}")

    payload = {
        "event_type": event.event_type,
        "lead":       event.lead.model_dump(),
        "source":     event.source,
        "received_at": datetime.now(timezone.utc).isoformat(),
    }

    msg = build_message(lead_id, event.event_type, payload, origin="webhook")

    # Inicializar contexto en Redis ANTES de publicar, y solo si el lead es
    # nuevo: si ya existe, un reenvío/duplicado no debe pisar su estado —
    # el Coordinador es quien decide si lo ignora o no.
    r = get_redis()
    if get_context(r, lead_id) is None:
        save_context(r, lead_id, {
            "lead_id":       lead_id,
            "estado":        "recibido",
            "agente_actual": "coordinador",
            "datos_lead":    event.lead.model_dump(),
            "timestamp_inicio": datetime.now(timezone.utc).isoformat(),
        })

    for attempt in range(2):
        try:
            ch = get_channel()
            publish(ch, "coordinator", msg)
            break
        except Exception as e:
            global rabbit_conn, rabbit_channel
            rabbit_conn, rabbit_channel = None, None
            if attempt == 1:
                log.error(f"Error publicando a RabbitMQ tras reintento: {e}")
                raise HTTPException(status_code=503, detail=f"Broker no disponible: {e}")

    # Registrar en PostgreSQL
    log_message(lead_id, "webhook", "coordinador", event.event_type,
                payload, "published", message_id=msg["message_id"])

    return {
        "accepted": True,
        "lead_id":  lead_id,
        "event":    event.event_type,
        "message_id": msg["message_id"],
    }


@app.post("/webhook/lead", status_code=202)
async def create_lead_direct(payload: DirectLeadPayload):
    """Endpoint simplificado para crear un lead directamente (útil para pruebas)."""
    lead_id = payload.lead_id
    log.info(f"Lead directo: {lead_id}")

    lead_dict = payload.model_dump()
    lead_dict["id"] = lead_id

    event_payload = {
        "event_type": "lead.created",
        "lead":       lead_dict,
        "source":     "api_direct",
        "received_at": datetime.now(timezone.utc).isoformat(),
    }

    msg = build_message(lead_id, "lead.created", event_payload, origin="webhook")

    # Inicializar el contexto en Redis ANTES de publicar, y solo si el lead es
    # nuevo: si ya existe (en curso o completado), un reenvío/duplicado no debe
    # pisar su estado — el Coordinador es quien decide si lo ignora o no.
    r = get_redis()
    if get_context(r, lead_id) is None:
        save_context(r, lead_id, {
            "lead_id":       lead_id,
            "estado":        "recibido",
            "agente_actual": "coordinador",
            "datos_lead":    lead_dict,
            "timestamp_inicio": datetime.now(timezone.utc).isoformat(),
        })

    for attempt in range(2):
        try:
            ch = get_channel()
            publish(ch, "coordinator", msg)
            break
        except Exception as e:
            global rabbit_conn, rabbit_channel
            rabbit_conn, rabbit_channel = None, None
            if attempt == 1:
                log.error(f"Error tras reintento: {e}")
                raise HTTPException(status_code=503, detail=str(e))

    log_message(lead_id, "webhook", "coordinador", "lead.created",
                event_payload, "published", message_id=msg["message_id"])

    return {"accepted": True, "lead_id": lead_id, "message_id": msg["message_id"]}


@app.get("/leads/{lead_id}/status")
def get_lead_status(lead_id: str):
    """Consulta el estado actual de un lead desde Redis."""
    try:
        r = get_redis()
        from messaging import get_context
        ctx = get_context(r, lead_id)
        if ctx is None:
            raise HTTPException(status_code=404, detail=f"Lead {lead_id} no encontrado")
        return ctx
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats")
def get_stats():
    """Estadísticas básicas del sistema."""
    try:
        r = get_redis()
        keys = r.keys("lead:context:*")
        return {
            "leads_en_redis": len(keys),
            "broker_conectado": rabbit_conn is not None and not rabbit_conn.is_closed,
        }
    except Exception as e:
        return {"error": str(e)}
