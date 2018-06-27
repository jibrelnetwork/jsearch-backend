from celery import Celery

from jsearch import settings


app = Celery('jsearch', broker=settings.JSEARCH_CELERY_BROKER, backend=settings.JSEARCH_CELERY_BACKEND)
app.autodiscover_tasks(['jsearch.api', 'jsearch.esparser', 'jsearch.syncer'])
