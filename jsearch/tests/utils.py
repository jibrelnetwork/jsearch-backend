import json
import logging
import subprocess
from concurrent import futures
from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory

logger = logging.getLogger(__name__)


def get_dump(connection_string):
    with TemporaryDirectory() as tmp_dir:
        dump_file = Path(tmp_dir) / "dump.json"
        cmd = ["python", "manage.py", "json_dump", "-db", connection_string, "-out", dump_file]
        process = subprocess.Popen(
            args=cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        try:
            outs, errs = process.communicate(timeout=120)
        except subprocess.TimeoutExpired:
            outs, errs = process.communicate()

        logger.info('Stats', extra={'stdout': outs, 'stderr': errs})

        if process.returncode != 0:
            raise RuntimeError('Process was exited with return code: %s', process.returncode)

        dump = dump_file.read_text()
        return json.loads(dump)


def sync_blocks(blocks, start_from=0, main_db_dsn="", raw_db_dsn=""):
    from jsearch.syncer.manager import sync_block
    with futures.ThreadPoolExecutor(max_workers=10) as executor:
        for block_hash in blocks:
            func = partial(
                sync_block,
                block_hash=block_hash,
                main_db_dsn=main_db_dsn,
                raw_db_dsn=raw_db_dsn
            )
            func()
            executor.submit(func)


def pprint_returned_value(func):
    def _wrapper(*args, **kwargs):
        from pprint import pprint
        result = func(*args, **kwargs)
        pprint(result)  # NOQA: T003
        return result

    return _wrapper
