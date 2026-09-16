"""CP-ANAL-002 — Agente de Análisis interpreta lead y devuelve resultado estructurado."""
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from conftest import get_redis, create_lead, redis_get_ctx, wait_for_state, pg_query, PIPELINE_WAIT


def test_CP_ANAL_002():
    lead_id = "L008"
    get_redis().delete(f"lead:context:{lead_id}")

    r = create_lead(lead_id, company="HealthCorp", sector="Salud",
                    annual_revenue=750000, description="inventario medico")
    assert r.status_code == 202
    wait_for_state(lead_id, ["planificacion_pendiente", "ejecucion_pendiente", "completado"], timeout=PIPELINE_WAIT)

    ctx = redis_get_ctx(lead_id)
    assert ctx is not None
    result = ctx.get("resultado_analysis")
    assert result is not None, "resultado_analysis no en contexto Redis"

    for campo in ["sector", "tipo_cliente", "interes_comercial", "datos_relevantes"]:
        assert campo in result, f"Campo '{campo}' faltante en resultado de análisis"

    assert result.get("sector", "").lower() in ("salud", "health", "no_especificado"), \
        f"Sector inesperado: {result.get('sector')}"

    rows = pg_query("SELECT * FROM decision_log WHERE lead_id=%s AND agente='analysis'", (lead_id,))
    assert len(rows) >= 1, "No hay decisión del agente analysis en decision_log"
