"""CP-INFRA-006 — Dashboard API /api/stats retorna estadísticas."""
import requests
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from conftest import DASHBOARD_URL


def test_CP_INFRA_006():
    r = requests.get(f"{DASHBOARD_URL}/api/stats", timeout=5)
    assert r.status_code == 200
    data = r.json()
    assert "leads_totales" in data
