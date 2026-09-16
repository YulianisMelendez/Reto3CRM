"""CP-VALI-002 — Agente Validador verifica cumplimiento de reglas empresariales."""
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from conftest import get_redis, create_lead, redis_get_ctx, wait_for_state, PIPELINE_WAIT


def test_CP_VALI_002():
    lead_id = "L016"
    get_redis().delete(f"lead:context:{lead_id}")

    r = create_lead(lead_id, sector="Educacion", annual_revenue=50000)
    assert r.status_code == 202
    wait_for_state(lead_id, ["supervision_pendiente", "completado"], timeout=PIPELINE_WAIT)

    ctx = redis_get_ctx(lead_id)
    result = ctx.get("resultado_validator") if ctx else None
    assert result is not None
    reglas = result.get("reglas_verificadas", [])
    assert len(reglas) >= 2, "Pocas reglas verificadas"
