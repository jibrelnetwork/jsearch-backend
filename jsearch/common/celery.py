from celery import Celery
from celery.schedules import crontab

from jsearch import settings


task_queues = {
    '*': 'default'
}


app = Celery('jsearch',
             broker=settings.JSEARCH_CELERY_BROKER,
             task_queues=task_queues)

app.autodiscover_tasks(['jsearch.api', 'jsearch.common'])
