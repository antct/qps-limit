## QPS Limit

Run functions under any limited rate using `multiprocessing` + `asyncio`

Available on Unix (i.e. Linux, MacOS) only, as the default multiprocessing start method is `fork`

### Installation

```bash
pip install qps-limit
```

or install manually via git

```bash
git clone git://github.com/antct/qps-limit.git qps-limit
cd qps-limit
python setup.py install
```

### Usage

```python
from qps_limit import Limiter

Limiter(
    func="an asynchronous function",
    params="a generator function yields args and kwargs",
    callback="a callback function that handles the return values of func",
    num_workers="number of processes, recommended <= number of CPUs",
    worker_max_qps="maximum qps per process, None means unlimited",
    ordered="return ordered results or not, the default option is False"
)
```

BTW: The wrapped function returns tuples `(idx, res)` consisting of the data index and the function return value. If `ordered=False` is set, the order of the returned values may be randomized for better performance.

### Quick Start

> 10 workers, each with a maximum qps of 10, to calculate the function value of `(1+1/n)^n`

> Assuming that `func` is a time-consuming function, it takes 1.0 seconds to execute

```python
import asyncio

from qps_limit import Limiter


async def func(n: int):
    await asyncio.sleep(1.0)
    return 1 + 1 / n, n


def params():
    for n in range(1, 1001):
        yield (), {"n": n}


def callback(r):
    return r[0] ** r[1]


f = Limiter(
    func=func,
    params=params,
    callback=callback,
    num_workers=10,
    worker_max_qps=10,
    ordered=True
)

for idx, r in f():
    print(idx, r)
```

```
receiver: 1000it [00:00, 861961.36it/s]
producer: 100%|██████████████████████████████| 1000/1000 [00:11<00:00, 87.07it/s]
consumer: 100%|██████████████████████████████| 1000/1000 [00:11<00:00, 87.11it/s]
```

### Best Practice

> Initialize resources that can not be `pickled` between processes

```python
resource = None

async def func(n):
    global resource
    if resource is None:
        resource = {}
```
