from celery import Celery
from celery.schedules import crontab

from jsearch import settings


task_queues = {
    '*': 'default'
}

beat_schedule = {
    'add-every-monday-morning': {
        'task': 'jsearch.esparser.tasks.run_contract_spider',
        'schedule': crontab(hour=3, minute=0),
    },
}


app = Celery('jsearch',
             broker=settings.JSEARCH_CELERY_BROKER,
             backend=settings.JSEARCH_CELERY_BACKEND,
             task_queues=task_queues,
             beat_schedule=beat_schedule)

app.autodiscover_tasks(['jsearch.api', 'jsearch.esparser', 'jsearch.common'])
