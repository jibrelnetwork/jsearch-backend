from concurrent.futures.process import ProcessPoolExecutor

from jsearch.utils import Singleton


class Executor(Singleton):

    def __init__(self):
        self.workers = 0
        self._executor = None

    def init(self, workers: int) -> None:
        if self.workers:
            raise ValueError('Executor already init')

        self.workers = workers
        self._executor = ProcessPoolExecutor(max_workers=workers)

    def get(self):
        return self._executor


executor = Executor()
