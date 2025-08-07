## QPS Limit

Run asynchronous functions with a rate limit using `multiprocessing` + `asyncio`

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
    num_coroutines="number of coroutines per worker, adjust according to the situation",
    max_qps="maximum qps, None means unlimited",
    ordered="return ordered results or not, the default option is False"
)
```

BTW: The wrapped function returns tuples `(idx, res)` consisting of the data index and the function return value. If `ordered=False` is set, the order of the returned values may be randomized for better performance.

### Quick Start

> 10 workers, each with a maximum qps of 10, to say "hello world"

```python
from qps_limit import Limiter


async def func(n: int):
    return "hello world {}".format(n)


def params():
    for n in range(1000):
        yield (), {"n": n}


f = Limiter(
    func=func,
    params=params,
    num_workers=10,
    num_coroutines=128,
    max_qps=100,
    ordered=False
)

for idx, res in f():
    print(idx, res)
```

```
receiver: 1000it [00:00, 1057032.26it/s]
producer: 100%|██████████████████████████████| 1000/1000 [00:11<00:00, 101.96it/s]
consumer: 100%|██████████████████████████████| 1000/1000 [00:11<00:00, 101.95it/s]
```

### Best Practice

> Initialize resources that can not be pickled

```python
resource = None

async def func(n):
    global resource
    # with process lock
    if resource is None:
        resource = {}
```

> Debugging code using only partial data

```python
Limiter(
    ...,
    max_steps=100
)
```

> Early termination if specific conditions are met

```python
i, max_i = 0, 100
for _, res in f():
    if # condition:
        if i > max_i:
            f.stop()
            break
        i += 1
```

> Safely write files with multiple processes

```python
import fcntl

writer = open('...', 'w+')

def callback(line):
    global writer
    fcntl.flock(writer, fcntl.LOCK_EX)
    writer.write('{}\n'.format(line))
    writer.flush()
    fcntl.flock(writer, fcntl.LOCK_UN)

f = Limiter(
    ...
    callback=callback
)
```
