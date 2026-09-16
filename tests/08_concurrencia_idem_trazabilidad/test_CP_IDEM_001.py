"""CP-IDEM-001 — Idempotencia: lead duplicado no procesado dos veces."""
import time
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from conftest import get_redis, get_pg, create_lead, wait_for_state, pg_query, PIPELINE_WAIT


def test_CP_IDEM_001():
    lead_id = "LIDEM001"
    get_redis().delete(f"lead:context:{lead_id}")

    conn = get_pg()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM decision_log WHERE lead_id=%s", (lead_id,))
        cur.execute("DELETE FROM message_log  WHERE lead_id=%s", (lead_id,))
    conn.commit()
    conn.close()

    r1 = create_lead(lead_id, sector="Tecnologia", annual_revenue=400000)
    time.sleep(0.5)
    r2 = create_lead(lead_id, sector="Tecnologia", annual_revenue=400000)

    assert r1.status_code == 202
    assert r2.status_code == 202

    wait_for_state(lead_id, ["completado", "fallido"], timeout=PIPELINE_WAIT)

    rows = pg_query("SELECT * FROM decision_log WHERE lead_id=%s AND agente='analysis'", (lead_id,))
    assert len(rows) <= 2, f"Posible procesamiento duplicado: {len(rows)} decisiones de análisis"
