"""CP-EXEC-002 — Agente Executor registra actividad de seguimiento."""
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from conftest import get_redis, create_lead, redis_get_ctx, wait_for_state, PIPELINE_WAIT


def test_CP_EXEC_002():
    lead_id = "L014"
    get_redis().delete(f"lead:context:{lead_id}")

    r = create_lead(lead_id, sector="Retail", annual_revenue=100000)
    assert r.status_code == 202
    wait_for_state(lead_id, ["validacion_pendiente", "completado"], timeout=PIPELINE_WAIT)

    ctx = redis_get_ctx(lead_id)
    result = ctx.get("resultado_executor") if ctx else None
    assert result is not None
    acciones = result.get("acciones_ejecutadas", [])
    nombres = [a.get("accion") for a in acciones]
    assert "registrar_actividad" in nombres, "Falta registro de actividad de seguimiento"
