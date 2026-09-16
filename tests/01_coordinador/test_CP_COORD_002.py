"""CP-COORD-002 — Coordinador selecciona primer agente correcto (analysis_queue)."""
import time
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from conftest import get_redis, create_lead, redis_get_ctx, pg_query


def test_CP_COORD_002():
    lead_id = "L002"
    get_redis().delete(f"lead:context:{lead_id}")

    r = create_lead(lead_id, sector="Tecnologia", annual_revenue=600000)
    assert r.status_code == 202

    time.sleep(2)
    ctx = redis_get_ctx(lead_id)
    assert ctx is not None, "Contexto no inicializado"

    estado_inicial = ctx.get("estado") or ""
    assert ("analisis" in estado_inicial.lower()
            or ctx.get("agente_actual") == "analysis"
            or ctx.get("resultado_analysis") is not None), \
        f"Estado inesperado: {estado_inicial}"

    rows = pg_query(
        "SELECT * FROM message_log WHERE lead_id=%s AND agente_destino='agente_analysis' AND origen='coordinador'",
        (lead_id,)
    )
    assert len(rows) >= 1, "No hay registro de despacho al agente_analysis"
    assert "estado" in ctx and "agente_actual" in ctx, "Contexto Redis incompleto"
