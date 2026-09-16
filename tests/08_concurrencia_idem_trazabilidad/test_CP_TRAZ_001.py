"""CP-TRAZ-001 — Trazabilidad completa desde creación hasta estado final."""
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from conftest import get_redis, create_lead, wait_for_state, pg_query, PIPELINE_WAIT


def test_CP_TRAZ_001():
    lead_id = "LTRAZ001"
    get_redis().delete(f"lead:context:{lead_id}")

    r = create_lead(lead_id, company="TraceTest", sector="Tecnologia",
                    annual_revenue=700000, email="trace@test.com")
    assert r.status_code == 202
    wait_for_state(lead_id, ["completado"], timeout=PIPELINE_WAIT)

    rows = pg_query(
        "SELECT DISTINCT agente_destino FROM message_log WHERE lead_id=%s AND origen='coordinador'",
        (lead_id,)
    )
    destinos = {r["agente_destino"] for r in rows}
    agentes_esperados = {"agente_analysis", "agente_planning", "agente_executor",
                         "agente_validator", "agente_supervisor"}
    encontrados = destinos & agentes_esperados
    assert len(encontrados) >= 3, \
        f"Solo se trazaron {len(encontrados)} agentes: {encontrados}"

    trace = pg_query("SELECT COUNT(*) AS cnt FROM lead_trace WHERE lead_id=%s", (lead_id,))
    assert trace[0]["cnt"] >= 3, "lead_trace con muy pocas entradas"
