## rate limit wrapper

```python
from rate_limit import MWrapper


async def f(i: int):
    return 'hello world {}'.format(i)


def gen_params():
    for i in range(1000):
        yield (), {"i": i}


engine = MWrapper(
    func=f,
    params=gen_params(),
    num_workers=10,
    worker_max_qps=10,
    streaming=False
)

for r in engine.start():
    print(r)
```

> elapsed time: 10.11s average qps: 98.96/100.00
