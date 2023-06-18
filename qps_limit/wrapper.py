import asyncio
import itertools
import multiprocessing
import time
from typing import Any, Callable, Coroutine, Dict, Iterable, Optional, Tuple

from aiolimiter import AsyncLimiter
from tqdm import tqdm


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
    max_qps: Optional[float],
    *,
    max_workers=128,
    callback: Callable = None,
    progress=False
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

    if progress:
        progress_bar = tqdm(total=jobs_cnt, desc=func.__name__, unit='req')

    async def worker():
        while not queue.empty():
            _idx, _param = await queue.get()
            result.append((_idx, await callback_func(*_param[0], **_param[1])))
            if progress:
                progress_bar.update(1)

    await asyncio.gather(*[worker() for _ in range(max_workers)])
    result.sort(key=lambda x: x[0])
    return [r for _, r in result]


def batch_run(
    func,
    params: Iterable[Tuple[Tuple, Dict]],
    max_qps: Optional[float],
    *,
    max_workers=128,
    callback: Callable = None,
    progress=False
):
    return asyncio.get_event_loop().run_until_complete(async_batch_run(**locals()))


async def async_streaming_batch_run(
    func: Callable[..., Coroutine[Any, Any, Any]],
    params: Iterable[Tuple[Tuple, Dict]],
    max_qps: Optional[float],
    *,
    max_workers=128,
    callback: Callable = None,
    progress=False
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

    for param in params:
        await queue.put(param)
        jobs_cnt += 1

    if progress:
        pbar = tqdm(total=jobs_cnt, desc=func.__name__, unit='req')

    lock = asyncio.Lock()

    async def worker():
        while not queue.empty():
            async with lock:
                _param = await queue.get()
                _res = await callback_func(*_param[0], **_param[1])
                await result.put(_res)
            if progress:
                pbar.update(1)

    asyncio.gather(*[worker() for _ in range(max_workers)])
    jobs_consume = 0
    while jobs_consume < jobs_cnt:
        res = await result.get()
        yield res
        jobs_consume += 1
    assert jobs_consume == jobs_cnt


def streaming_batch_run(
    func,
    params: Iterable[Tuple[Tuple, Dict]],
    max_qps: Optional[float],
    *,
    max_workers=128,
    callback: Callable = None,
    progress=False
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


class MWrapper():

    def __init__(
        self,
        func: Callable[..., Coroutine[Any, Any, Any]],
        params: Iterable[Tuple[Tuple, Dict]],
        num_workers: int = 1,
        worker_max_qps: float = None,
        streaming: bool = False,
        callback: Callable = None,
        progress: bool = True,
        verbose: bool = False
    ):

        self.func = func
        self.params = params
        self.num_workers = num_workers
        self.worker_max_qps = worker_max_qps
        self.streaming = streaming
        self.callback = callback
        self.progress = progress
        self.verbose = verbose

        self.workers = []
        self.queue = multiprocessing.Queue()
        self.count_iterator, self.param_iterator = itertools.tee(self.params, 2)
        self.count = 0
        for _ in self.count_iterator:
            self.count += 1

        for mod in range(self.num_workers):
            self.workers.append(
                multiprocessing.Process(
                    target=self._worker,
                    args=(
                        mod,
                    )
                )
            )

    def _worker(self, mod):
        def make_iterators():
            def iterator():
                for idx, (args, kwargs) in enumerate(self.param_iterator):
                    if idx % self.num_workers == mod:
                        yield idx, (args, kwargs)
            a_iter, b_iter = itertools.tee(iterator(), 2)
            return (a for a, _ in a_iter), (b for _, b in b_iter)

        idx_iterator, param_iterator = make_iterators()

        if not self.streaming:
            idxs = [idx for idx in idx_iterator]
            results = batch_run(
                func=self.func,
                params=param_iterator,
                max_qps=self.worker_max_qps,
                max_workers=16 if not self.worker_max_qps else max(16, self.worker_max_qps),
                callback=self.callback,
                progress=self.progress
            )
            assert len(idxs) == len(results)
            for idx, res in zip(idxs, results):
                self.queue.put((idx, res))
        else:
            result_iterator = streaming_batch_run(
                func=self.func,
                params=param_iterator,
                max_qps=self.worker_max_qps,
                max_workers=16 if not self.worker_max_qps else max(16, self.worker_max_qps),
                callback=self.callback,
                progress=self.progress
            )
            for idx, res in zip(idx_iterator, result_iterator):
                self.queue.put((idx, res))

    def start(self):
        start_time = time.time()
        for worker in self.workers:
            worker.start()
        consume = 0
        while consume < self.count:
            yield self.queue.get()
            consume += 1
        assert consume == self.count
        end_time = time.time()
        if self.verbose:
            print('elapsed time: {:.2f}s average qps: {:.2f}/{:.2f}'.format(
                end_time - start_time,
                self.count / (end_time - start_time),
                self.worker_max_qps * self.num_workers if self.worker_max_qps else float("inf"))
            )
