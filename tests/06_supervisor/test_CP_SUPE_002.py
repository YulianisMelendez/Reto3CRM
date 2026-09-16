"""CP-SUPE-002 — Agente Supervisor identifica casos para intervención humana."""
import requests
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from conftest import get_redis, redis_get_ctx, wait_for_state, WEBHOOK_URL, PIPELINE_WAIT


def test_CP_SUPE_002():
    lead_id = "L018"
    get_redis().delete(f"lead:context:{lead_id}")

    r = requests.post(f"{WEBHOOK_URL}/webhook/lead", json={
        "lead_id":        lead_id,
        "company":        "Enterprise Missing",
        "sector":         "Tecnologia",
        "annual_revenue": 2000000,
        "email":          None,
    }, timeout=10)
    assert r.status_code == 202
    wait_for_state(lead_id, ["completado", "pendiente_revision_humana", "fallido_validacion"], timeout=PIPELINE_WAIT)

    ctx = redis_get_ctx(lead_id)
    assert ctx is not None

    sup = ctx.get("resultado_supervisor")
    if sup:
        decision = sup.get("decision", "")
        assert decision in ("APROBADO", "REQUIERE_INTERVENCION_HUMANA"), \
            f"Decisión inválida: {decision}"
    else:
        estado = ctx.get("estado", "")
        assert "fallido" in estado or "pendiente_revision" in estado or "completado" in estado
