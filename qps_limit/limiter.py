import asyncio
import itertools
import logging
import math
import multiprocessing
import time
from queue import Empty, Full
from typing import Any, Callable, Coroutine, Dict, Iterable, Optional, Tuple, Union

import faster_fifo
from aiolimiter import AsyncLimiter
from tqdm import tqdm


def _get_limiter(max_qps: float):
    time_period = 0.1
    min_max_rate = 2.0
    max_rate = max_qps * time_period
    if max_rate < min_max_rate:
        time_period = min_max_rate * time_period / max_rate
        max_rate = min_max_rate
    return AsyncLimiter(max_rate=max_rate, time_period=time_period)


def _get_queue(queue: Union[multiprocessing.Queue, faster_fifo.Queue]):
    if isinstance(queue, faster_fifo.Queue):
        while True:
            try:
                return queue.get()
            except Empty:
                pass
    else:
        return queue.get()


def _put_queue(queue: Union[multiprocessing.Queue, faster_fifo.Queue], value: Any):
    if isinstance(queue, faster_fifo.Queue):
        while True:
            try:
                return queue.put(value)
            except Full:
                pass
    else:
        return queue.put(value)


async def _async_run(
    func: Callable[..., Coroutine[Any, Any, Any]],
    params: Iterable[Tuple[Tuple, Dict]],
    callback: Optional[Callable] = None,
    max_qps: Optional[float] = None,
    max_coroutines: int = 128,
    job_produce: Optional[multiprocessing.Value] = None,
    job_consume: Optional[multiprocessing.Value] = None,
    worker_produce: Optional[multiprocessing.Value] = None,
    worker_consume: Optional[multiprocessing.Value] = None,
    worker_waiting: Optional[multiprocessing.Event] = None,
    idx_mapping: Optional[Callable] = None,
    res_queue: Optional[Union[multiprocessing.Queue, faster_fifo.Queue]] = None
):
    if max_qps is not None:
        limiter = _get_limiter(max_qps)

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

    def inc(value: multiprocessing.Value):
        if value:
            with value.get_lock():
                value.value += 1

    async def producer():
        for idx, param in enumerate(params):
            await queue.put((idx, param))
            inc(job_produce)

    async def consumer():
        while not queue.empty():
            _idx, _param = await queue.get()
            _res = await callback_func(*_param[0], **_param[1])
            if res_queue:
                _put_queue(res_queue, (idx_mapping(_idx) if idx_mapping else _idx, _res))
            inc(job_consume)

    await producer()

    inc(worker_produce)

    if worker_waiting:
        worker_waiting.wait()

    await asyncio.gather(*[consumer() for _ in range(max_coroutines)])

    inc(worker_consume)


def _run(
    func: Callable[..., Coroutine[Any, Any, Any]],
    params: Iterable[Tuple[Tuple, Dict]],
    callback: Optional[Callable] = None,
    max_qps: Optional[float] = None,
    max_coroutines: int = 128,
    job_produce: Optional[multiprocessing.Value] = None,
    job_consume: Optional[multiprocessing.Value] = None,
    worker_produce: Optional[multiprocessing.Value] = None,
    worker_consume: Optional[multiprocessing.Value] = None,
    worker_waiting: Optional[multiprocessing.Event] = None,
    idx_mapping: Optional[Callable] = None,
    res_queue: Optional[Union[multiprocessing.Queue, faster_fifo.Queue]] = None
):
    asyncio.new_event_loop().run_until_complete(_async_run(**locals()))


class Limiter():

    def __init__(
        self,
        func: Callable[..., Coroutine[Any, Any, Any]],
        params: Callable,
        callback: Optional[Callable] = None,
        num_workers: int = 1,
        worker_max_qps: Optional[float] = None,
        ordered: bool = True,
        verbose: bool = False,
        warmup_steps: int = 1,
        max_steps: Optional[int] = None,
        max_coroutines: int = 128,
        faster_queue: bool = True
    ) -> Callable:
        if not asyncio.iscoroutinefunction(func):
            raise RuntimeError('func must be a coroutine function')

        self.func = func
        self.params = params
        self.callback = callback
        self.num_workers = num_workers
        self.worker_max_qps = worker_max_qps
        self.ordered = ordered
        self.verbose = verbose
        self.warmup_steps = warmup_steps
        self.max_steps = max_steps
        self.max_coroutines = max_coroutines
        self.faster_queue = faster_queue

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%m/%d/%Y %H:%M:%S"
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        try:
            multiprocessing.set_start_method('fork')
        except RuntimeError:
            if self.verbose:
                self.logger.error("multiprocessing set_start_method error")

        self.job_produce = multiprocessing.Value('i', 0)
        self.job_consume = multiprocessing.Value('i', 0)
        self.worker_produce = multiprocessing.Value('i', 0)
        self.worker_consume = multiprocessing.Value('i', 0)
        self.worker_waiting = multiprocessing.Event()
        if self.faster_queue:
            self.res_queue = faster_fifo.Queue()
        else:
            self.res_queue = multiprocessing.Queue()

    def _warmup(self):
        if self.verbose:
            self.logger.info("warmup worker nodes with {} data".format(self.warmup_steps))
        warmup_param_iterator = itertools.islice(self.params(), self.warmup_steps)
        warmup_start_time = time.time()
        _run(
            func=self.func,
            params=warmup_param_iterator,
            callback=None,
            max_qps=None,
            max_coroutines=1
        )
        self.warmup_worker_time = (time.time() - warmup_start_time) / self.warmup_steps
        if self.worker_max_qps is None:
            self.dynamic_coroutines = self.max_coroutines
        else:
            self.dynamic_coroutines = math.ceil(self.worker_max_qps * math.ceil(self.warmup_worker_time))
            self.dynamic_coroutines = min(self.max_coroutines, self.dynamic_coroutines)
        if self.verbose:
            self.logger.info("warmup worker time: {:.2f}s -> set dynamic coroutine num: {}".format(
                self.warmup_worker_time, self.dynamic_coroutines
            ))

    def _worker(self, mod: int):
        _run(
            func=self.func,
            params=itertools.islice(self.params(), mod, self.max_steps, self.num_workers),
            callback=self.callback,
            max_qps=self.worker_max_qps,
            max_coroutines=self.dynamic_coroutines,
            job_produce=self.job_produce,
            job_consume=self.job_consume,
            worker_produce=self.worker_produce,
            worker_consume=self.worker_consume,
            worker_waiting=self.worker_waiting,
            idx_mapping=lambda idx: idx * self.num_workers + mod,
            res_queue=self.res_queue
        )

    def __call__(self):
        self._warmup()

        workers = [multiprocessing.Process(target=self._worker, args=(mod, )) for mod in range(self.num_workers)]
        for worker in workers:
            worker.start()

        receiver = tqdm(desc='receiver')
        while True:
            receiver.update(self.job_produce.value - receiver.n)
            if self.worker_produce.value == self.num_workers:
                break
        receiver.close()
        total = self.job_produce.value

        producer_bar_event = multiprocessing.Event()
        consumer_bar_event = multiprocessing.Event()

        def main_producer():
            producer_bar = tqdm(desc='producer', total=total, position=0)
            while producer_bar.n < total:
                producer_bar.update(self.job_consume.value - producer_bar.n)
            producer_bar.refresh()
            producer_bar_event.wait()
            producer_bar.close()
            consumer_bar_event.set()

        multiprocessing.Process(target=main_producer).start()
        self.worker_waiting.set()

        def main_consumer():
            done = 0
            ordered_res_map = {}
            consumer_bar = tqdm(desc='consumer', total=total, position=1)
            while done < total:
                if self.ordered:
                    while done not in ordered_res_map:
                        idx, res = _get_queue(self.res_queue)
                        ordered_res_map[idx] = res
                    yield (done, ordered_res_map[done])
                    del ordered_res_map[done]
                else:
                    yield _get_queue(self.res_queue)
                done += 1
                consumer_bar.update(1)
            consumer_bar.refresh()
            producer_bar_event.set()
            consumer_bar_event.wait()
            consumer_bar.close()

            for worker in workers:
                worker.join()

        return main_consumer()

    def stop(self):
        [worker.kill() for worker in multiprocessing.active_children()]
