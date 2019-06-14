from typing import Any

from jsearch import settings
from jsearch.common import services
from jsearch.common.healthcheck_app import make_app


class ApiService(services.ApiService):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault('port', settings.NOTABLES_WORKER_API_PORT)
        kwargs.setdefault('app_maker', make_app)

        super(ApiService, self).__init__(*args, **kwargs)
