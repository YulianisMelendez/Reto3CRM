"""CP-EXEC-001 — Agente Executor realiza acciones sobre SuiteCRM/simulado."""
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from conftest import get_redis, create_lead, redis_get_ctx, wait_for_state, PIPELINE_WAIT


def test_CP_EXEC_001():
    lead_id = "L013"
    get_redis().delete(f"lead:context:{lead_id}")

    r = create_lead(lead_id, sector="Salud", annual_revenue=500000)
    assert r.status_code == 202
    wait_for_state(lead_id, ["validacion_pendiente", "completado"], timeout=PIPELINE_WAIT)

    ctx = redis_get_ctx(lead_id)
    assert ctx is not None
    result = ctx.get("resultado_executor")
    assert result is not None, "resultado_executor no en contexto"

    acciones = result.get("acciones_ejecutadas", [])
    assert len(acciones) >= 1, "Sin acciones ejecutadas"

    nombres = [a.get("accion") for a in acciones]
    assert "actualizar_prioridad" in nombres, "Falta acción actualizar_prioridad"
    assert "asignar_asesor"       in nombres, "Falta acción asignar_asesor"
