"""CP-INFRA-005 — Dashboard API /api/leads responde correctamente."""
import requests
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from conftest import DASHBOARD_URL


def test_CP_INFRA_005():
    r = requests.get(f"{DASHBOARD_URL}/api/leads", timeout=5)
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)
