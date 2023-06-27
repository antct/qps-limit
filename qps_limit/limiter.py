import collections
import itertools
import math
import multiprocessing
import time
from typing import Any, Callable, Coroutine, Optional

from tqdm import tqdm

from .run import batch_run, streaming_batch_run


class Limiter():

    def __init__(
        self,
        func: Callable[..., Coroutine[Any, Any, Any]],
        params: Callable,
        num_workers: int = 1,
        worker_max_qps: Optional[float] = None,
        streaming: bool = False,
        callback: Optional[Callable] = None,
        progress: bool = True,
        ordered: bool = True,
        verbose: bool = False,
        max_workers: int = 128,
        warmup_steps: int = 1
    ) -> Callable:
        multiprocessing.set_start_method('fork')

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
        counter = itertools.count()
        collections.deque(zip(self.count_iterator, counter), maxlen=0)
        self.count = next(counter)
        if self.verbose:
            print("find {} data, warmup workers with {} data".format(self.count, warmup_steps))

        self.param_iterator, warmup_param_iterator = itertools.tee(self.param_iterator, 2)
        warmup_param_iterator = itertools.islice(warmup_param_iterator, warmup_steps)
        warmup_start_time = time.time()
        batch_run(
            func=self.func,
            params=warmup_param_iterator,
            max_qps=None,
            max_workers=1
        )
        warmup_end_time = time.time()
        avg_worker_time = (warmup_end_time - warmup_start_time) / warmup_steps
        if self.worker_max_qps is None:
            self.max_workers = max_workers
        else:
            self.max_workers = min(max_workers, self.worker_max_qps * math.ceil(avg_worker_time))
        if self.verbose:
            print("avg worker time: {:.2f}s -> set worker num: {}".format(avg_worker_time, self.max_workers))

        if self.ordered:
            self.dict = multiprocessing.Manager().dict()
        else:
            self.queue = multiprocessing.Queue()

        self.workers = []
        if self.progress:
            self.progress_queue = multiprocessing.Queue()
            self.workers.append(multiprocessing.Process(target=self._progress_worker))
        else:
            self.progress_queue = None

        for mod in range(self.num_workers):
            self.workers.append(multiprocessing.Process(target=self._worker, args=(mod,)))

    def _progress_worker(self):
        progress_bar = tqdm(total=self.count, desc=self.func.__name__)
        progress_cnt = 0
        while progress_cnt < self.count:
            progress_bar.update(self.progress_queue.get())
            progress_cnt += 1

    def _worker(self, mod: int):
        def make_worker_iterator():
            for idx, (args, kwargs) in enumerate(self.param_iterator):
                if idx % self.num_workers == mod:
                    yield args, kwargs

        batch_run_func = batch_run if not self.streaming else streaming_batch_run
        for idx, res in batch_run_func(
            func=self.func,
            params=make_worker_iterator(),
            max_qps=self.worker_max_qps,
            max_workers=self.max_workers,
            callback=self.callback,
            progress_queue=self.progress_queue
        ):
            real_idx = idx * self.num_workers + mod
            if self.ordered:
                self.dict[real_idx] = res
            else:
                self.queue.put((real_idx, res))

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
