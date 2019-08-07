import pytest
from pytest_mock import MockFixture


@pytest.fixture
def disable_metrics_setup(mocker: MockFixture) -> None:
    # Metrics setup contributes to a 'prometheus_client.registry.REGISTRY'
    # singleton. If same metric is contributed twice, `ValueError` is raised.

    mocker.patch('jsearch.common.stats.setup_api_metrics')
    mocker.patch('jsearch.common.stats.setup_syncer_metrics')
    mocker.patch('jsearch.common.stats.setup_pending_syncer_metrics')
