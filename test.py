import aiohttp

from qps_limit import Limiter


async def func(n):
    url = 'https://postman-echo.com/get?n={}'.format(n)
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            data = await resp.json()
            return data['args']['n']


def params():
    for n in range(100):
        yield (), {"n": n}


f = Limiter(
    func=func,
    params=params,
    num_workers=1,
    num_coroutines=128,
    max_qps=2,
    ordered=False
)

for idx, res in f():
    print(idx, res)
