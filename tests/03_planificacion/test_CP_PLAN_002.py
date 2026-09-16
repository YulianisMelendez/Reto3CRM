"""CP-PLAN-002 — Agente de Planificación establece prioridad coherente con análisis."""
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from conftest import get_redis, create_lead, redis_get_ctx, wait_for_state, PIPELINE_WAIT


def test_CP_PLAN_002():
    lead_id = "L011"
    get_redis().delete(f"lead:context:{lead_id}")

    r = create_lead(lead_id, sector="Tecnologia", annual_revenue=2000000)
    assert r.status_code == 202
    wait_for_state(lead_id, ["ejecucion_pendiente", "completado"], timeout=PIPELINE_WAIT)

    ctx = redis_get_ctx(lead_id)
    plan = ctx.get("resultado_planning") if ctx else None
    assert plan is not None, "Sin resultado de planificación"
    assert plan.get("prioridad") in ("alta", "media", "baja", "critica"), \
        f"Prioridad inválida: {plan.get('prioridad')}"
