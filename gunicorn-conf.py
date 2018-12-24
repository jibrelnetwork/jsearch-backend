import os

accesslog = '-'
access_log_format = '%t [ID:%{request-id}i] [RA:%a] [PID:%P] [C:%s] [S:%b] [T:%D] [HT:%{host}i] [R:%r]'
bind = "0.0.0.0:{port!s}".format(
    port=os.getenv("PORT", 8080)
)
loglevel = 'info'
worker_class = 'aiohttp.worker.GunicornWebWorker'
