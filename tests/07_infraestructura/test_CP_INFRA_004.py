"""CP-INFRA-004 — Redis: contextos de lead con estructura válida."""
import json
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from conftest import get_redis


def test_CP_INFRA_004():
    r = get_redis()
    keys = r.keys("lead:context:*")
    if keys:
        raw = r.get(keys[0])
        ctx = json.loads(raw)
        assert "lead_id" in ctx
        assert "estado"  in ctx
