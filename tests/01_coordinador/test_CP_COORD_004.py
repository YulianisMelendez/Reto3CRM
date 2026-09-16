"""CP-COORD-004 — Coordinador detiene flujo y registra error por INSUFFICIENT_DATA."""
import requests
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from conftest import get_redis, redis_get_ctx, wait_for_state, pg_query, WEBHOOK_URL, PIPELINE_WAIT


def test_CP_COORD_004():
    lead_id = "L004"
    get_redis().delete(f"lead:context:{lead_id}")

    r = requests.post(f"{WEBHOOK_URL}/webhook/lead", json={
        "lead_id": lead_id, "company": "Empresa Sin Datos",
        "sector": None, "annual_revenue": None, "email": "nodata@test.com",
    }, timeout=10)
    assert r.status_code == 202

    state = wait_for_state(lead_id, ["fallido_analisis", "completado"], timeout=PIPELINE_WAIT)
    ctx = redis_get_ctx(lead_id)
    assert ctx is not None, "Contexto no encontrado"

    estado = ctx.get("estado", "")
    if estado == "fallido_analisis":
        assert ctx.get("error_codigo") == "INSUFFICIENT_DATA", \
            f"Código de error inesperado: {ctx.get('error_codigo')}"
        rows = pg_query(
            "SELECT * FROM decision_log WHERE lead_id=%s AND agente IN ('planning','executor','validator','supervisor')",
            (lead_id,)
        )
        assert len(rows) == 0, f"Pipeline continuó tras error: {[r['agente'] for r in rows]}"
    else:
        rows = pg_query("SELECT * FROM message_log WHERE lead_id=%s AND resultado='error'", (lead_id,))
        assert len(rows) >= 1, "No se registró error en message_log"
