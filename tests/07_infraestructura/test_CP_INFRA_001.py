"""CP-INFRA-001 — RabbitMQ: exchanges leads.direct y leads.dlx activos."""
import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from conftest import rmq_exchange_exists


def test_CP_INFRA_001():
    assert rmq_exchange_exists("leads.direct"), "Exchange 'leads.direct' no encontrado"
    assert rmq_exchange_exists("leads.dlx"),    "Exchange 'leads.dlx' no encontrado"
