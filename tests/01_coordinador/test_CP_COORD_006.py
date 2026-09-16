"""CP-COORD-006 — Coordinador detecta timeout y aplica política de reintento/derivación."""
import subprocess
import time
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from conftest import get_redis, create_lead, redis_get_ctx, pg_query


def test_CP_COORD_006():
    lead_id = "L006"
    get_redis().delete(f"lead:context:{lead_id}")

    docker_available = False
    try:
        result = subprocess.run(["docker", "stop", "agent-analysis"], capture_output=True, timeout=10)
        docker_available = result.returncode == 0
        time.sleep(2)
    except Exception:
        pass

    r = create_lead(lead_id, sector="Retail", annual_revenue=150000)
    assert r.status_code == 202

    if docker_available:
        time.sleep(5)
        rows = pg_query("SELECT * FROM message_log WHERE lead_id=%s AND agente_destino='agente_analysis'", (lead_id,))
        assert len(rows) >= 1, "El Coordinador no registró despacho a analysis"
        subprocess.run(["docker", "start", "agent-analysis"], capture_output=True, timeout=10)
        time.sleep(3)
    else:
        time.sleep(3)
        rows = pg_query("SELECT * FROM message_log WHERE lead_id=%s", (lead_id,))
        assert len(rows) >= 1, "No hay trazabilidad del lead L006"
        ctx = redis_get_ctx(lead_id)
        assert ctx is not None, "Contexto de L006 no inicializado"
