## Rate Limit

Run functions under any limited rate using `multiprocessing` + `asyncio`

### Installation

```bash
pip install qps-limit
```

or install manually via git

```bash
git clone git://github.com/antct/rate-limit.git rate-limit
cd rate-limit
python setup.py install
```

### Usage

```python
from qps_limit import Limiter

f = Limiter(
    func="an asynchronous function",
    params="a generator function yields args and kwargs",
    num_workers="number of processes, recommended <= number of CPUs",
    worker_max_qps="maximum qps per process, None means unlimited",
    streaming="stream data processing, useful when the memory is limited",
    callback="a callback function that handles the return values of func",
    progress="display a progress bar",
    ordered="return ordered results or not"
)
```

BTW: The wrapped function returns a structure `(idx, res)` consisting of an index of the data and the function return value. If `ordered=False` is set, the order of the returned values may be randomized for better performance.

### Example

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
    num_workers=10,
    worker_max_qps=10,
    streaming=False,
    callback=callback,
    progress=True,
    ordered=True,
    verbose=False
)

for idx, r in f():
    print(idx, r)
```