"""CP-CONCUR-001 — Procesamiento concurrente de múltiples leads."""
import threading
import time
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from conftest import get_redis, create_lead, redis_get_ctx, PIPELINE_WAIT


def test_CP_CONCUR_001():
    leads = [f"LC{i:03d}" for i in range(1, 6)]
    errors = []

    def send_lead(lid):
        try:
            get_redis().delete(f"lead:context:{lid}")
            r = create_lead(lid, sector="Tecnologia", annual_revenue=300000)
            if r.status_code != 202:
                errors.append(f"{lid}: HTTP {r.status_code}")
        except Exception as e:
            errors.append(f"{lid}: {e}")

    threads = [threading.Thread(target=send_lead, args=(lid,)) for lid in leads]
    for t in threads: t.start()
    for t in threads: t.join()

    assert len(errors) == 0, f"Errores en concurrencia: {errors}"

    time.sleep(PIPELINE_WAIT)
    procesados = sum(
        1 for lid in leads
        if (ctx := redis_get_ctx(lid)) and ctx.get("estado")
    )
    assert procesados >= 3, f"Solo {procesados}/{len(leads)} leads procesados concurrentemente"
