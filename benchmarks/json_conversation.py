import asyncio
import lzma
import time
from collections import defaultdict

import asyncpg
import requests
import orjson
import postgres_gateway


measures = defaultdict(lambda: [])


async def init_bench():
    r = requests.get('https://github.com/ijl/orjson/blob/master/data/github.json.xz?raw=true')
    contents = lzma.decompress(r.content)
    conn = await asyncpg.connect('postgres://postgres:postgres@0.0.0.0:5432/postgres')
    await conn.execute('create table test_json(id uuid primary key default gen_random_uuid(), value jsonb not null)')
    await conn.execute('insert into test_json(value) values($1)', contents.decode())


def measure(name: str):
    def inner(func):
        async def deco():
            t0 = time.time()
            result = await func()
            t1 = time.time()
            measures[name].append(t1 - t0)
            return result
        return deco
    return inner


@measure('asyncpg + orjson')
async def try_asyncpg_and_orjson():
    conn = await asyncpg.connect('postgres://postgres:postgres@0.0.0.0:5432/postgres')
    value = await conn.fetchrow('select value from test_json')
    return orjson.loads(value[0])


@measure('postgres_gateway')
async def try_postgres_gateway():
    conn = await postgres_gateway.connect('postgres://postgres:postgres@0.0.0.0:5432/postgres')
    value = await conn.fetchrow('select value from test_json')
    return value[0]


async def main():
    assert await try_asyncpg_and_orjson() == await try_postgres_gateway()
    for _ in range(1_000):
        await try_asyncpg_and_orjson()
        await try_postgres_gateway()


if __name__ == '__main__':
    asyncio.run(main())
