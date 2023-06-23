import asyncio
import multiprocessing
from typing import Any, Callable, Coroutine, Dict, Iterable, Optional, Tuple

from aiolimiter import AsyncLimiter


def get_limiter(max_qps: float):
    time_period = 0.1
    max_rate = max_qps * time_period
    if max_rate < 1:
        time_period = time_period / max_rate
        max_rate = 1
    return AsyncLimiter(max_rate=max_rate, time_period=time_period)


async def async_batch_run(
    func: Callable[..., Coroutine[Any, Any, Any]],
    params: Iterable[Tuple[Tuple, Dict]],
    *,
    max_qps: Optional[float] = None,
    max_workers: int = 128,
    callback: Optional[Callable] = None,
    progress_queue: Optional[multiprocessing.Queue] = None
):
    if max_qps is not None:
        limiter = get_limiter(max_qps)

        async def limited_func(*args, **kwargs):
            async with limiter:
                return await func(*args, **kwargs)
    else:
        limited_func = func

    async def callback_func(*args, **kwargs):
        if callback:
            return callback(await limited_func(*args, **kwargs))
        else:
            return await limited_func(*args, **kwargs)

    result = []
    queue = asyncio.Queue()
    jobs_cnt = 0

    for idx, param in enumerate(params):
        await queue.put((idx, param))
        jobs_cnt += 1

    async def worker():
        while not queue.empty():
            _idx, _param = await queue.get()
            result.append((_idx, await callback_func(*_param[0], **_param[1])))
            if progress_queue:
                progress_queue.put_nowait(1)

    await asyncio.gather(*[worker() for _ in range(max_workers)])
    assert len(result) == jobs_cnt
    return result


def batch_run(
    func: Callable[..., Coroutine[Any, Any, Any]],
    params: Iterable[Tuple[Tuple, Dict]],
    *,
    max_qps: Optional[float] = None,
    max_workers: int = 128,
    callback: Optional[Callable] = None,
    progress_queue: Optional[multiprocessing.Queue] = None
):
    return asyncio.get_event_loop().run_until_complete(async_batch_run(**locals()))


async def async_streaming_batch_run(
    func: Callable[..., Coroutine[Any, Any, Any]],
    params: Iterable[Tuple[Tuple, Dict]],
    *,
    max_qps: Optional[float] = None,
    max_workers: int = 128,
    callback: Optional[Callable] = None,
    progress_queue: Optional[multiprocessing.Queue] = None
):
    if max_qps is not None:
        limiter = get_limiter(max_qps)

        async def limited_func(*args, **kwargs):
            async with limiter:
                return await func(*args, **kwargs)
    else:
        limited_func = func

    async def callback_func(*args, **kwargs):
        if callback:
            return callback(await limited_func(*args, **kwargs))
        else:
            return await limited_func(*args, **kwargs)

    queue = asyncio.Queue()
    result = asyncio.Queue()
    jobs_cnt = 0

    for idx, param in enumerate(params):
        await queue.put((idx, param))
        jobs_cnt += 1

    async def worker():
        while not queue.empty():
            _idx, _param = await queue.get()
            _res = await callback_func(*_param[0], **_param[1])
            await result.put((_idx, _res))
            if progress_queue:
                progress_queue.put_nowait(1)

    asyncio.gather(*[worker() for _ in range(max_workers)])
    jobs_consume = 0
    while jobs_consume < jobs_cnt:
        yield await result.get()
        jobs_consume += 1
    assert jobs_consume == jobs_cnt


def streaming_batch_run(
    func: Callable[..., Coroutine[Any, Any, Any]],
    params: Iterable[Tuple[Tuple, Dict]],
    *,
    max_qps: Optional[float] = None,
    max_workers: int = 128,
    callback: Optional[Callable] = None,
    progress_queue: Optional[multiprocessing.Queue] = None
):
    async_generator = async_streaming_batch_run(**locals())

    def iter_over_async(ait, loop):
        ait = ait.__aiter__()

        async def get_next():
            try:
                return False, await ait.__anext__()
            except StopAsyncIteration:
                return True, None
        while True:
            done, obj = loop.run_until_complete(get_next())
            if done:
                break
            yield obj

    loop = asyncio.get_event_loop()
    sync_generator = iter_over_async(async_generator, loop)
    return sync_generator