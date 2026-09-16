"""CP-ANAL-001 — Agente de Análisis recibe tarea exclusivamente del Coordinador."""
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from conftest import get_redis, create_lead, wait_for_state, pg_query, PIPELINE_WAIT


def test_CP_ANAL_001():
    lead_id = "L007"
    get_redis().delete(f"lead:context:{lead_id}")

    r = create_lead(lead_id, sector="Tecnologia", annual_revenue=400000)
    assert r.status_code == 202
    wait_for_state(lead_id, ["analisis_completado", "planificacion_pendiente", "completado"], timeout=PIPELINE_WAIT)

    rows = pg_query(
        "SELECT * FROM message_log WHERE lead_id=%s AND agente_destino='agente_analysis'",
        (lead_id,)
    )
    assert len(rows) >= 1, "No hay registro de despacho a agente_analysis"
    for row in rows:
        assert row["origen"] == "coordinador", \
            f"Origen inesperado: {row['origen']} (debe ser 'coordinador')"
