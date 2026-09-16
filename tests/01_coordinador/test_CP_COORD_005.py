"""CP-COORD-005 — Coordinador mantiene contexto completo y lo actualiza progresivamente."""
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from conftest import get_redis, create_lead, redis_get_ctx, wait_for_state, pg_query, PIPELINE_WAIT


def test_CP_COORD_005():
    lead_id = "L005"
    get_redis().delete(f"lead:context:{lead_id}")

    r = create_lead(lead_id, sector="Manufactura", annual_revenue=250000, description="inventario y ventas")
    assert r.status_code == 202

    wait_for_state(lead_id, ["completado", "fallido", "supervision_pendiente"], timeout=PIPELINE_WAIT)

    ctx = redis_get_ctx(lead_id)
    assert ctx is not None
    assert "lead_id"                     in ctx, "Falta lead_id en contexto"
    assert "estado"                      in ctx, "Falta estado en contexto"
    assert "timestamp_inicio"            in ctx, "Falta timestamp_inicio"
    assert "timestamp_ultima_actualizacion" in ctx, "Falta timestamp_ultima_actualizacion"
    assert "resultado_analysis"          in ctx, "Resultado de análisis no guardado en contexto"

    rows = pg_query("SELECT * FROM message_log WHERE lead_id=%s AND origen='coordinador'", (lead_id,))
    assert len(rows) >= 2, f"Solo {len(rows)} registros del coordinador (esperados >=2)"
