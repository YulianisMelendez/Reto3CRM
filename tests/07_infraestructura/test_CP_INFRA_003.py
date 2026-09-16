"""CP-INFRA-003 — PostgreSQL: vista lead_trace retorna datos coherentes."""
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from conftest import pg_query


def test_CP_INFRA_003():
    rows = pg_query("SELECT * FROM lead_trace LIMIT 5")
    if rows:
        assert "lead_id" in rows[0], "Columna lead_id faltante en lead_trace"
        assert "origen"  in rows[0], "Columna origen faltante en lead_trace"
