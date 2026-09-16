"""CP-COORD-003 — Solo el Coordinador publica en colas de agentes (modelo centralizado)."""
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from conftest import get_redis, create_lead, wait_for_state, pg_query, PIPELINE_WAIT


def test_CP_COORD_003():
    lead_id = "L003"
    get_redis().delete(f"lead:context:{lead_id}")

    r = create_lead(lead_id, sector="Salud", annual_revenue=800000, description="crm integration")
    assert r.status_code == 202

    wait_for_state(lead_id, ["completado", "fallido"], timeout=PIPELINE_WAIT)

    rows_asig = pg_query(
        "SELECT * FROM message_log WHERE lead_id=%s "
        "AND agente_destino NOT IN ('coordinador','sistema') "
        "AND origen != 'coordinador' AND origen != 'webhook'",
        (lead_id,)
    )
    assert len(rows_asig) == 0, \
        f"Agentes publicaron asignaciones directas: {[r['origen']+'→'+r['agente_destino'] for r in rows_asig]}"
