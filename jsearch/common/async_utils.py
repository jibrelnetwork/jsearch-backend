from typing import Sequence, Mapping, List

from jsearch.typing import AnyCoroutine, AnyCoroutineMaker


def chain_dependent_coros(
        items: Sequence[Mapping],
        item_id_key: str,
        create_task: AnyCoroutineMaker,
) -> List[AnyCoroutine]:

    """Generates sequence of linked tasks.

    If any `item` mapping with given `item_id_key` appears multiple times in a
    sequence, it will be chained preserving order.

    Warnings:
         * `items` must be already ordered.
         * Return value will be *unordered* and meant to be used exactly in
         asyncio.gather(*result).
    """
    last_tasks = {}

    for item in items:
        item_id = item[item_id_key]
        task = create_task(item)

        task_before = last_tasks.get(item_id)

        if task_before:
            task = _make_chain(task_before, task)

        last_tasks[item_id] = task

    return list(last_tasks.values())


async def _make_chain(task_before: AnyCoroutine, task: AnyCoroutine) -> None:
    await task_before
    await task
