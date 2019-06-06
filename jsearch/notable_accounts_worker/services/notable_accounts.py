from typing import List

import mode

from jsearch import settings

from jsearch.common import services
from jsearch.service_bus import service_bus


database = services.DatabaseService(dsn=settings.JSEARCH_MAIN_DB)


class NotableAccountsService(mode.Service):
    def on_init_dependencies(self) -> List[mode.Service]:
        return [service_bus, database]
