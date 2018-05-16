from jsearch.compiler.utils import compile_contract, get_contract
from jsearch.compiler.celery import app


DEFAULT_OPTIMIZER_RUNS = 200


@app.task
def compile_contract_task(source_code, contract_name, compiler, optimization_enabled):
    compile_result = compile_contract(
        source_code,
        contract_name,
        compiler,
        optimization_enabled,
        DEFAULT_OPTIMIZER_RUNS)
    return compile_result


@app.task
def get_token_info(address):
    pass