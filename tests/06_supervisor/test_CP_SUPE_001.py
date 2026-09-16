"""CP-SUPE-001 — Agente Supervisor consolida resultados y toma decisión final."""
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from conftest import get_redis, create_lead, redis_get_ctx, wait_for_state, pg_query, PIPELINE_WAIT


def test_CP_SUPE_001():
    lead_id = "L017"
    get_redis().delete(f"lead:context:{lead_id}")

    r = create_lead(lead_id, company="BigCorp", sector="Tecnologia",
                    annual_revenue=1500000, email="big@corp.com")
    assert r.status_code == 202
    wait_for_state(lead_id, ["completado", "pendiente_revision_humana"], timeout=PIPELINE_WAIT)

    ctx = redis_get_ctx(lead_id)
    assert ctx is not None
    result = ctx.get("resultado_supervisor")
    assert result is not None, "resultado_supervisor no en contexto"

    assert "decision"    in result, "Falta decision en resultado supervisor"
    assert "consolidado" in result, "Falta consolidado en resultado supervisor"
    assert result.get("pipeline_completo") is True

    rows = pg_query("SELECT * FROM decision_log WHERE lead_id=%s AND agente='supervisor'", (lead_id,))
    assert len(rows) >= 1, "Sin decisión del supervisor en decision_log"
