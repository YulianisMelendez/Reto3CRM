import json
import uuid
import logging
import time
import os
from datetime import datetime, timezone
from typing import Optional

import pika
import redis
import psycopg2
from psycopg2.extras import Json

# ── Configuración desde variables de entorno ──────────────────────────────────
RABBITMQ_URL  = os.getenv("RABBITMQ_URL",  "amqp://agentuser:agentpass@rabbitmq:5672/")
REDIS_URL     = os.getenv("REDIS_URL",     "redis://redis:6379/0")
PG_DSN        = os.getenv("PG_DSN",        "host=postgres port=5432 dbname=agentdb user=agentuser password=agentpass")

EXCHANGE      = "leads.direct"
DLX           = "leads.dlx"

QUEUE_MAP = {
    "coordinator":         "coordinator_queue",
    "analysis":            "analysis_queue",
    "planning":            "planning_queue",
    "executor":            "executor_queue",
    "validator":           "validator_queue",
    "supervisor":          "supervisor_queue",
    "coordinator_response":"coordinator_responses",
}

log = logging.getLogger("shared")


# ── RabbitMQ ──────────────────────────────────────────────────────────────────
def get_rabbitmq_connection(retries: int = 10, delay: float = 3.0) -> pika.BlockingConnection:
    params = pika.URLParameters(RABBITMQ_URL)
    params.heartbeat = 60
    params.blocked_connection_timeout = 30
    for i in range(retries):
        try:
            conn = pika.BlockingConnection(params)
            log.info("RabbitMQ conectado")
            return conn
        except Exception as e:
            log.warning(f"RabbitMQ no disponible ({e}), reintento {i+1}/{retries}")
            time.sleep(delay)
    raise RuntimeError("No se pudo conectar a RabbitMQ")


def declare_topology(channel: pika.adapters.blocking_connection.BlockingChannel):
    """Declara exchanges y colas de forma idempotente."""
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="direct", durable=True)
    channel.exchange_declare(exchange=DLX, exchange_type="direct", durable=True)

    dlq_args = {}
    agent_args = {
        "x-dead-letter-exchange":    DLX,
        "x-dead-letter-routing-key": "dlq",
        "x-message-ttl":             300000,
    }

    channel.queue_declare(queue="leads.dlq", durable=True, arguments=dlq_args)
    channel.queue_bind(exchange=DLX, queue="leads.dlq", routing_key="dlq")

    for routing_key, queue_name in QUEUE_MAP.items():
        args = agent_args if routing_key != "coordinator_response" else {"x-message-ttl": 300000}
        channel.queue_declare(queue=queue_name, durable=True, arguments=args)
        channel.queue_bind(exchange=EXCHANGE, queue=queue_name, routing_key=routing_key)


def build_message(lead_id: str, task_type: str, payload: dict, origin: str = "coordinator") -> dict:
    return {
        "message_id": str(uuid.uuid4()),
        "lead_id":    lead_id,
        "task_type":  task_type,
        "payload":    payload,
        "origin":     origin,
        "timestamp":  datetime.now(timezone.utc).isoformat(),
    }


def publish(channel, routing_key: str, message: dict, priority: int = 0):
    body = json.dumps(message).encode()
    props = pika.BasicProperties(
        delivery_mode=2,
        content_type="application/json",
        message_id=message.get("message_id", str(uuid.uuid4())),
    )
    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key=routing_key,
        body=body,
        properties=props,
    )
    log.info(f"[PUBLISH] {routing_key} → lead={message.get('lead_id')} type={message.get('task_type')}")


# ── Redis ─────────────────────────────────────────────────────────────────────
def get_redis() -> redis.Redis:
    return redis.from_url(REDIS_URL, decode_responses=True)


def context_key(lead_id: str) -> str:
    return f"lead:context:{lead_id}"


def get_context(r: redis.Redis, lead_id: str) -> Optional[dict]:
    raw = r.get(context_key(lead_id))
    return json.loads(raw) if raw else None


def save_context(r: redis.Redis, lead_id: str, ctx: dict, ttl: int = 86400):
    ctx["timestamp_ultima_actualizacion"] = datetime.now(timezone.utc).isoformat()
    r.set(context_key(lead_id), json.dumps(ctx), ex=ttl)


def update_context(r: redis.Redis, lead_id: str, updates: dict):
    ctx = get_context(r, lead_id) or {}
    ctx.update(updates)
    save_context(r, lead_id, ctx)


# ── PostgreSQL ────────────────────────────────────────────────────────────────
def get_pg():
    return psycopg2.connect(PG_DSN)


def log_message(lead_id: str, origen: str, agente_destino: str, tipo_evento: str,
                mensaje: dict, resultado: str = "pending", error_codigo: str = None,
                message_id: str = None):
    try:
        conn = get_pg()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO message_log
                       (lead_id, origen, agente_destino, tipo_evento, message_id,
                        mensaje, resultado, error_codigo, fecha_hora)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())""",
                    (lead_id, origen, agente_destino, tipo_evento,
                     message_id or str(uuid.uuid4()),
                     Json(mensaje), resultado, error_codigo)
                )
        conn.close()
    except Exception as e:
        log.error(f"Error log_message: {e}")


def log_decision(lead_id: str, agente: str, decision: dict):
    try:
        conn = get_pg()
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO decision_log (lead_id, agente, decision, fecha_hora) VALUES (%s, %s, %s, NOW())",
                    (lead_id, agente, Json(decision))
                )
        conn.close()
    except Exception as e:
        log.error(f"Error log_decision: {e}")
