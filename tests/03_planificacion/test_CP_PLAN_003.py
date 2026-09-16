"""CP-PLAN-003 — Agente de Planificación define actividades paralelas."""
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from conftest import get_redis, create_lead, redis_get_ctx, wait_for_state, PIPELINE_WAIT


def test_CP_PLAN_003():
    lead_id = "L012"
    get_redis().delete(f"lead:context:{lead_id}")

    r = create_lead(lead_id, sector="Manufactura", annual_revenue=300000)
    assert r.status_code == 202
    wait_for_state(lead_id, ["ejecucion_pendiente", "completado"], timeout=PIPELINE_WAIT)

    ctx = redis_get_ctx(lead_id)
    plan = ctx.get("resultado_planning") if ctx else None
    assert plan is not None
    assert "tareas_paralelas" in plan, "Falta definición de tareas_paralelas"
