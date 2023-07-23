import itertools
import json
import logging
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
        callback: Optional[Callable] = None,
        num_workers: int = 1,
        worker_max_qps: Optional[float] = None,
        streaming: bool = False,
        ordered: bool = True,
        verbose: bool = False,
        warmup_steps: int = 1,
        max_coroutines: int = 128
    ) -> Callable:
        self.func = func
        self.params = params
        self.callback = callback
        self.num_workers = num_workers
        self.worker_max_qps = worker_max_qps
        self.streaming = streaming
        self.ordered = ordered
        self.verbose = verbose
        self.warmup_steps = warmup_steps
        self.max_coroutines = max_coroutines

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

        if self.verbose:
            self.logger.info("warmup worker nodes with {} data".format(self.warmup_steps))
        warmup_param_iterator = itertools.islice(self.params(), self.warmup_steps)
        warmup_start_time = time.time()
        batch_run(
            func=self.func,
            params=warmup_param_iterator,
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

        if self.ordered:
            self.res_map = multiprocessing.Manager().dict()
        else:
            self.res_queue = multiprocessing.Queue()

        self.job_value = multiprocessing.Value('i', 0)
        self.worker_value = multiprocessing.Value('i', 0)
        self.worker_event = multiprocessing.Event()
        self.job_queue = multiprocessing.Queue()
        self.worker_time = multiprocessing.Value('d', 0)

        self.workers = []
        for mod in range(self.num_workers):
            self.workers.append(multiprocessing.Process(target=self._worker, args=(mod, )))

    def _progress_worker(self):
        progress_bar = tqdm(total=self.job_count, desc=self.func.__name__)
        progress_cnt = 0
        while progress_cnt < self.job_count:
            progress_bar.update(self.job_queue.get())
            progress_cnt += 1
        progress_bar.close()
        with self.worker_time.get_lock():
            self.worker_time.value = progress_bar.last_print_t - progress_bar.start_t

    def _worker(self, mod: int):
        def make_worker_iterator():
            for idx, (args, kwargs) in enumerate(self.params()):
                if idx % self.num_workers == mod:
                    yield args, kwargs

        batch_run_func = batch_run if not self.streaming else streaming_batch_run
        for idx, res in batch_run_func(
            func=self.func,
            params=make_worker_iterator(),
            callback=self.callback,
            max_qps=self.worker_max_qps,
            max_coroutines=self.dynamic_coroutines,
            job_queue=self.job_queue,
            job_value=self.job_value,
            worker_value=self.worker_value,
            worker_event=self.worker_event
        ):
            real_idx = idx * self.num_workers + mod
            if self.ordered:
                self.res_map[real_idx] = res
            else:
                self.res_queue.put((real_idx, res))

    def __call__(self):
        start_time = time.time()
        for worker in self.workers:
            worker.start()

        data_bar = tqdm(desc='data', disable=not self.verbose)
        while True:
            data_bar.update(self.job_value.value - data_bar.n)
            if self.worker_value.value == self.num_workers:
                break
        data_bar.close()
        if self.verbose:
            self.logger.info(
                "receive {} data from {} worker nodes".format(self.job_value.value, self.worker_value.value)
            )

        self.job_count = self.job_value.value
        progress_worker = multiprocessing.Process(target=self._progress_worker)
        progress_worker.start()
        self.worker_event.set()

        job_done = 0
        while job_done < self.job_count:
            if self.ordered:
                while job_done not in self.res_map:
                    pass
                yield (job_done, self.res_map[job_done])
                del self.res_map[job_done]
            else:
                yield self.res_queue.get()
            job_done += 1
        assert job_done == self.job_count

        for worker in self.workers:
            worker.join()
        progress_worker.join()

        if self.verbose:
            verbose_info = {}
            verbose_info['limiter'] = {
                'func_name': self.func.__name__,
                'num_workers': self.num_workers,
                'worker_max_qps': self.worker_max_qps,
                'streaming': self.streaming,
                'ordered': self.ordered,
                'warmup_steps': self.warmup_steps,
                'max_coroutines': self.max_coroutines,
            }
            verbose_info['runtime'] = {
                'warmup_worker_time': self.warmup_worker_time,
                'dynamic_coroutines': self.dynamic_coroutines,
                'data_count': self.job_count,
                'data_time': data_bar.last_print_t - data_bar.start_t,
                'worker_time': self.worker_time.value,
                'elapsed_time': time.time() - start_time,
                'average_qps': self.job_count / self.worker_time.value if self.worker_time.value > 0 else float("inf"),
                'expected_qps': self.worker_max_qps * self.num_workers if self.worker_max_qps else float("inf")
            }
            self.logger.info(json.dumps(verbose_info, indent=4))
