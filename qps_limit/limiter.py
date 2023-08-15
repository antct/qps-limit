import itertools
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
        cutoff_steps: Optional[int] = None,
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
        self.cutoff_steps = cutoff_steps
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

        self.res_queue = multiprocessing.Queue()

        self.job_value = multiprocessing.Value('i', 0)
        self.job_queue = multiprocessing.Queue()
        self.worker_value = multiprocessing.Value('i', 0)
        self.worker_event = multiprocessing.Event()

        self.workers = [multiprocessing.Process(target=self._worker, args=(mod, )) for mod in range(self.num_workers)]

    def _worker(self, mod: int):
        batch_run_func = batch_run if not self.streaming else streaming_batch_run
        for idx, res in batch_run_func(
            func=self.func,
            params=itertools.islice(self.params(), mod, self.cutoff_steps, self.num_workers),
            callback=self.callback,
            max_qps=self.worker_max_qps,
            max_coroutines=self.dynamic_coroutines,
            job_queue=self.job_queue,
            job_value=self.job_value,
            worker_value=self.worker_value,
            worker_event=self.worker_event
        ):
            real_idx = idx * self.num_workers + mod
            self.res_queue.put((real_idx, res))

    def __call__(self):
        for worker in self.workers:
            worker.start()

        receiver = tqdm(desc='receiver')
        while True:
            receiver.update(self.job_value.value - receiver.n)
            if self.worker_value.value == self.num_workers:
                break
        receiver.close()
        if self.verbose:
            self.logger.info(
                "receive {} data from {} worker nodes".format(self.job_value.value, self.worker_value.value)
            )
        self.job_count = self.job_value.value

        producer_event = multiprocessing.Event()
        producer_value = multiprocessing.Value('i', 0)
        consumer_event = multiprocessing.Event()

        def _producer():
            producer = tqdm(desc='producer', total=self.job_count, position=0)
            while producer.n < self.job_count:
                producer.update(self.job_queue.get())
                with producer_value.get_lock():
                    producer_value.value = producer.n
            producer.refresh()
            producer_event.wait()
            producer.close()
            consumer_event.set()

        multiprocessing.Process(target=_producer).start()
        self.worker_event.set()

        res_map = {}
        job_done = 0
        consumer = tqdm(desc='consumer', total=self.job_count, position=1)
        while job_done < self.job_count:
            if consumer.n > producer_value.value:
                continue
            if self.ordered:
                while job_done not in res_map:
                    idx, res = self.res_queue.get()
                    res_map[idx] = res
                yield (job_done, res_map[job_done])
                del res_map[job_done]
            else:
                yield self.res_queue.get()
            job_done += 1
            consumer.update(1)
        consumer.refresh()
        producer_event.set()
        consumer_event.wait()
        consumer.close()

        for worker in self.workers:
            worker.join()
