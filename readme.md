## rate limit wrapper

> 10 workers, each with a maximum qps of 10, to calculate the function value of `(1+1/n)^n`

```python
from rate_limit import MWrapper


async def func(n: int):
    return 1 + 1 / n, n


def params():
    for n in range(1, 1001):
        yield (), {"n": n}


def callback(r):
    return r[0] ** r[1]


engine = MWrapper(
    func=func,
    params=params(),
    num_workers=10,
    worker_max_qps=10,
    streaming=False,
    callback=callback,
    progress=True
)

for i, r in engine.start():
    print(i, r)
```

> elapsed time: 10.11s average qps: 98.96/100.00
