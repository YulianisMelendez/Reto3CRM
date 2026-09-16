"""CP-VALI-001 — Agente Validador verifica coherencia y campos obligatorios."""
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from conftest import get_redis, create_lead, redis_get_ctx, wait_for_state, pg_query, PIPELINE_WAIT


def test_CP_VALI_001():
    lead_id = "L015"
    get_redis().delete(f"lead:context:{lead_id}")

    r = create_lead(lead_id, company="FullData Corp", sector="Tecnologia",
                    annual_revenue=500000, email="full@data.com")
    assert r.status_code == 202
    wait_for_state(lead_id, ["supervision_pendiente", "completado"], timeout=PIPELINE_WAIT)

    ctx = redis_get_ctx(lead_id)
    assert ctx is not None
    result = ctx.get("resultado_validator")
    assert result is not None, "resultado_validator no en contexto"

    assert "validado"           in result
    assert "reglas_verificadas" in result
    assert len(result["reglas_verificadas"]) >= 1

    rows = pg_query("SELECT * FROM decision_log WHERE lead_id=%s AND agente='validator'", (lead_id,))
    assert len(rows) >= 1, "Sin decisión del validador en decision_log"
