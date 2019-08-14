import os

accesslog = '-'
access_log_format = '%t [ID:%{request-id}i] [RA:%a] [PID:%P] [C:%s] [S:%b] [T:%D] [HT:%{host}i] [R:%r]'
bind = "0.0.0.0:{port!s}".format(
    port=os.environ["PORT"]
)
loglevel = 'info'
worker_class = 'aiohttp.worker.GunicornWebWorker'
workers = os.getenv("WORKERS", 10)
