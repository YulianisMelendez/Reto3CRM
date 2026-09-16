import json
import os
import subprocess
import sys
import time
from typing import Optional

import requests
import redis as redis_lib
import psycopg2
import psycopg2.extras
import pika

# ── Configuración ──────────────────────────────────────────────────────────────
WEBHOOK_URL   = os.getenv("WEBHOOK_URL",   "http://localhost:8080")
DASHBOARD_URL = os.getenv("DASHBOARD_URL", "http://localhost:8888")
RABBITMQ_API  = os.getenv("RABBITMQ_API",  "http://localhost:15672")
RABBITMQ_USER = "agentuser"
RABBITMQ_PASS = "agentpass"
REDIS_URL     = os.getenv("REDIS_URL",     "redis://localhost:6379/0")
PG_DSN        = os.getenv("PG_DSN",        "host=localhost port=5433 dbname=agentdb user=agentuser password=agentpass")
PIPELINE_WAIT = float(os.getenv("PIPELINE_WAIT", "15"))


def get_redis() -> redis_lib.Redis:
    return redis_lib.from_url(REDIS_URL, decode_responses=True)


def get_pg():
    return psycopg2.connect(PG_DSN)


def redis_get_ctx(lead_id: str) -> Optional[dict]:
    raw = get_redis().get(f"lead:context:{lead_id}")
    return json.loads(raw) if raw else None


def pg_query(sql: str, params=None) -> list:
    conn = get_pg()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params or ())
        rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def rmq_queue_count(queue_name: str) -> int:
    try:
        r = requests.get(
            f"{RABBITMQ_API}/api/queues/%2F/{queue_name}",
            auth=(RABBITMQ_USER, RABBITMQ_PASS), timeout=5
        )
        return r.json().get("messages", 0) if r.status_code == 200 else -1
    except Exception:
        return -1


def rmq_exchange_exists(name: str) -> bool:
    try:
        r = requests.get(
            f"{RABBITMQ_API}/api/exchanges/%2F/{name}",
            auth=(RABBITMQ_USER, RABBITMQ_PASS), timeout=5
        )
        return r.status_code == 200
    except Exception:
        return False


def create_lead(lead_id: str, company: str = "Test SA", sector: str = "Tecnologia",
                annual_revenue: float = 500000, email: str = None,
                description: str = "") -> requests.Response:
    payload = {
        "lead_id":        lead_id,
        "company":        company,
        "sector":         sector,
        "annual_revenue": annual_revenue,
        "email":          email or f"{lead_id.lower()}@test.com",
        "description":    description,
    }
    return requests.post(f"{WEBHOOK_URL}/webhook/lead", json=payload, timeout=10)


def wait_for_state(lead_id: str, expected_states: list, timeout: float = PIPELINE_WAIT) -> Optional[str]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        ctx = redis_get_ctx(lead_id)
        if ctx:
            estado = ctx.get("estado", "")
            if any(estado == s or estado.startswith(s) for s in expected_states):
                return estado
        time.sleep(0.5)
    ctx = redis_get_ctx(lead_id)
    return ctx.get("estado") if ctx else None
