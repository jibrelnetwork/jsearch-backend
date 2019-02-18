from concurrent.futures.process import ProcessPoolExecutor

executor = None


def init_executor(workers: int):
    global executor

    if executor:
        raise ValueError('Executor already was inited')

    executor = ProcessPoolExecutor(max_workers=workers)


def get_executor():
    if not executor:
        raise ValueError('Executor does not inited')
    return executor
