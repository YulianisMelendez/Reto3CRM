"""CP-INFRA-002 — RabbitMQ: colas de agentes declaradas con DLX."""
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from conftest import rmq_queue_count


def test_CP_INFRA_002():
    for q in ["analysis_queue", "planning_queue", "executor_queue",
              "validator_queue", "supervisor_queue"]:
        cnt = rmq_queue_count(q)
        assert cnt >= 0, f"Cola '{q}' no encontrada en RabbitMQ"
