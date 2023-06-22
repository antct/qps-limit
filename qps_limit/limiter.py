import asyncio
import collections
import itertools
import math
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
    idxs: Iterable[int],
    params: Iterable[Tuple[Tuple, Dict]],
    max_qps: Optional[float],
    *,
    max_workers: int = 128,
    callback: Callable = None,
    progress_queue: multiprocessing.Queue = None
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

    for idx, param in zip(idxs, params):
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
    idxs: Iterable[int],
    params: Iterable[Tuple[Tuple, Dict]],
    max_qps: Optional[float],
    *,
    max_workers: int = 128,
    callback: Callable = None,
    progress_queue: multiprocessing.Queue = None
):
    return asyncio.get_event_loop().run_until_complete(async_batch_run(**locals()))


async def async_streaming_batch_run(
    func: Callable[..., Coroutine[Any, Any, Any]],
    idxs: Iterable[int],
    params: Iterable[Tuple[Tuple, Dict]],
    max_qps: Optional[float],
    *,
    max_workers: int = 128,
    callback: Callable = None,
    progress_queue: multiprocessing.Queue = None
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

    for idx, param in zip(idxs, params):
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
    idxs: Iterable[int],
    params: Iterable[Tuple[Tuple, Dict]],
    max_qps: Optional[float],
    *,
    max_workers: int = 128,
    callback: Callable = None,
    progress_queue: multiprocessing.Queue = None
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


class Limiter():

    def __init__(
        self,
        func: Callable[..., Coroutine[Any, Any, Any]],
        params: Callable,
        num_workers: int = 1,
        worker_max_qps: float = None,
        streaming: bool = False,
        callback: Callable = None,
        progress: bool = True,
        ordered: bool = True,
        verbose: bool = False
    ) -> Callable:
        self.func = func
        self.params = params
        self.num_workers = num_workers
        self.worker_max_qps = worker_max_qps
        self.streaming = streaming
        self.callback = callback
        self.progress = progress
        self.ordered = ordered
        self.verbose = verbose

        self.count_iterator, self.param_iterator = itertools.tee(self.params(), 2)
        self.count = 0
        counter = itertools.count()
        collections.deque(zip(self.count_iterator, counter), maxlen=0)
        self.count = next(counter)

        self.param_iterator, warmup_param_iterator = itertools.tee(self.param_iterator)
        warmup_cnt = 1
        warmup_idx_iterator = (i for i in range(warmup_cnt))
        warmup_param_iterator = itertools.islice(warmup_param_iterator, warmup_cnt)
        warmup_start_time = time.time()
        if self.verbose:
            print("warm up workers with {} data".format(warmup_cnt))
        batch_run(
            func=self.func,
            idxs=warmup_idx_iterator,
            params=warmup_param_iterator,
            max_qps=None,
            max_workers=1
        )
        warmup_end_time = time.time()
        avg_worker_time = (warmup_end_time - warmup_start_time) / warmup_cnt
        max_workers_num = 128
        if self.worker_max_qps is None:
            self.max_workers = max_workers_num
        else:
            self.max_workers = min(max_workers_num, self.worker_max_qps * math.ceil(avg_worker_time))
        if self.verbose:
            print("avg worker time: {:.2f}s -> set worker num: {}".format(avg_worker_time, self.max_workers))

        if self.ordered:
            self.dict = multiprocessing.Manager().dict()
        else:
            self.queue = multiprocessing.Queue()

        self.workers = []
        if self.progress:
            self.progress_queue = multiprocessing.Queue()

            def _progress_worker():
                progress_bar = tqdm(total=self.count, desc=self.func.__name__)
                progress_cnt = 0
                while progress_cnt < self.count:
                    progress_bar.update(self.progress_queue.get())
                    progress_cnt += 1
                progress_bar.close()
            self.workers.append(multiprocessing.Process(target=_progress_worker, args=()))
        else:
            self.progress_queue = None

        for mod in range(self.num_workers):
            self.workers.append(multiprocessing.Process(target=self._worker, args=(mod,)))

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
            results = batch_run(
                func=self.func,
                idxs=idx_iterator,
                params=param_iterator,
                max_qps=self.worker_max_qps,
                max_workers=self.max_workers,
                callback=self.callback,
                progress_queue=self.progress_queue
            )
            for idx, res in results:
                if self.ordered:
                    self.dict[idx] = res
                else:
                    self.queue.put((idx, res))
        else:
            result_iterator = streaming_batch_run(
                func=self.func,
                idxs=idx_iterator,
                params=param_iterator,
                max_qps=self.worker_max_qps,
                max_workers=self.max_workers,
                callback=self.callback,
                progress_queue=self.progress_queue
            )
            for idx, res in result_iterator:
                if self.ordered:
                    self.dict[idx] = res
                else:
                    self.queue.put((idx, res))

    def __call__(self):
        start_time = time.time()
        for worker in self.workers:
            worker.start()
        consume = 0
        while consume < self.count:
            if self.ordered:
                while consume not in self.dict:
                    pass
                yield (consume, self.dict[consume])
                del self.dict[consume]
            else:
                yield self.queue.get()
            consume += 1
        assert consume == self.count
        for worker in self.workers:
            worker.join()
        end_time = time.time()
        if self.verbose:
            print('elapsed time: {:.2f}s average qps: {:.2f}/{:.2f}'.format(
                end_time - start_time,
                self.count / (end_time - start_time),
                self.worker_max_qps * self.num_workers if self.worker_max_qps else float("inf"))
            )
