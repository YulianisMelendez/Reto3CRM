"""CP-COORD-001 — Coordinador procesa correctamente la creación de un Lead."""
import time
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from conftest import get_redis, create_lead, redis_get_ctx, wait_for_state, pg_query, PIPELINE_WAIT


def test_CP_COORD_001():
    lead_id = "L001"
    get_redis().delete(f"lead:context:{lead_id}")

    r = create_lead(lead_id, company="Empresa ABC", sector="Tecnologia",
                    annual_revenue=600000, email="abc@empresa.com")
    assert r.status_code == 202, f"HTTP {r.status_code}: {r.text}"

    time.sleep(2)
    ctx = redis_get_ctx(lead_id)
    assert ctx is not None, "Contexto no encontrado en Redis"

    state = wait_for_state(lead_id, ["analisis_pendiente", "analisis_completado", "completado"], timeout=10)
    assert state is not None, "El pipeline no avanzó"

    rows = pg_query("SELECT * FROM message_log WHERE lead_id=%s AND origen='coordinador'", (lead_id,))
    assert len(rows) >= 1, "No hay registros del coordinador en message_log"

    trace = pg_query("SELECT * FROM lead_trace WHERE lead_id=%s", (lead_id,))
    assert len(trace) >= 1, "lead_trace vacío para el lead"
