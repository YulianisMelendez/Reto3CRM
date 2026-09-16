"""CP-PLAN-001 — Agente de Planificación define tareas y asigna asesor comercial."""
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from conftest import get_redis, create_lead, redis_get_ctx, wait_for_state, pg_query, PIPELINE_WAIT


def test_CP_PLAN_001():
    lead_id = "L010"
    get_redis().delete(f"lead:context:{lead_id}")

    r = create_lead(lead_id, sector="Tecnologia", annual_revenue=600000)
    assert r.status_code == 202
    wait_for_state(lead_id, ["ejecucion_pendiente", "validacion_pendiente", "completado"], timeout=PIPELINE_WAIT)

    ctx = redis_get_ctx(lead_id)
    assert ctx is not None
    result = ctx.get("resultado_planning")
    assert result is not None, "resultado_planning no en contexto Redis"

    assert "asesor_asignado"     in result, "Falta asesor_asignado"
    assert "prioridad"           in result, "Falta prioridad"
    assert "tareas_secuenciales" in result, "Falta tareas_secuenciales"
    assert len(result["tareas_secuenciales"]) >= 1, "Sin tareas planificadas"

    rows = pg_query("SELECT * FROM decision_log WHERE lead_id=%s AND agente='planning'", (lead_id,))
    assert len(rows) >= 1, "Sin decisión de planning en decision_log"
