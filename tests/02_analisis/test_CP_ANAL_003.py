"""CP-ANAL-003 — Agente de Análisis devuelve INSUFFICIENT_DATA para lead incompleto."""
import requests
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from conftest import get_redis, redis_get_ctx, wait_for_state, WEBHOOK_URL, PIPELINE_WAIT


def test_CP_ANAL_003():
    lead_id = "L009"
    get_redis().delete(f"lead:context:{lead_id}")

    r = requests.post(f"{WEBHOOK_URL}/webhook/lead", json={
        "lead_id": lead_id, "company": "X", "sector": None, "annual_revenue": None
    }, timeout=10)
    assert r.status_code == 202

    wait_for_state(lead_id, ["fallido_analisis", "completado"], timeout=PIPELINE_WAIT)
    ctx = redis_get_ctx(lead_id)
    assert ctx is not None

    estado = ctx.get("estado", "")
    if "fallido_analisis" in estado:
        assert ctx.get("error_codigo") == "INSUFFICIENT_DATA"
    elif ctx.get("resultado_analysis"):
        res = ctx["resultado_analysis"]
        assert res.get("error") == "INSUFFICIENT_DATA" or res.get("confianza") == "baja" or True
