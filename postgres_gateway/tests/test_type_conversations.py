import uuid

import pytest as pytest

import postgres_gateway
from constants import POSTGRES_DSN

pytestmark = pytest.mark.asyncio


async def test_ints():
    conn = await postgres_gateway.connect(POSTGRES_DSN)
    row = await conn.fetchrow("""
        SELECT -8::int2 AS one, 2::int4 AS two, 3::int8 AS three;
    """)
    assert dict(row) == {'one': -8, 'two': 2, 'three': 3}


async def test_strings():
    conn = await postgres_gateway.connect(POSTGRES_DSN)
    row = await conn.fetchrow("""
        SELECT ''::TEXT AS one, 'qwe'::varchar(8) AS two, 'asd'::char(3) AS three
    """)
    assert dict(row) == {'one': '', 'two': 'qwe', 'three': 'asd'}


async def test_bools():
    conn = await postgres_gateway.connect(POSTGRES_DSN)
    row = await conn.fetchrow("""
        SELECT true AS one, false AS two, null::bool AS three
    """)
    assert dict(row) == {'one': True, 'two': False, 'three': None}


async def test_uuid():
    conn = await postgres_gateway.connect(POSTGRES_DSN)
    uid = uuid.uuid4()
    await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS abc(
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name TEXT
        );
    """)
    await conn.execute(f"""
        INSERT INTO abc(id, name) VALUES ('{uid}', 'test_uuid');
    """)
    row = await conn.fetchrow(f"""
        SELECT * FROM abc WHERE name = 'test_uuid'
    """)
    assert dict(row) == {'id': uid, 'name': 'test_uuid'}


@pytest.mark.parametrize('tpe', ['jsonb', 'json'])
async def test_jsons(tpe):
    obj = '{"a": "b", "c": [1,  2,3]}'
    conn = await postgres_gateway.connect(POSTGRES_DSN)
    row = await conn.fetchrow(f"""
        select
            'null'::{tpe}             AS nil,
            '{obj}'::{tpe}            AS one,
            '2'::{tpe}                AS two,
            '"123"'::{tpe}            AS three,
            '["ab", 1, false]'::{tpe} AS four;
    """)
    assert dict(row) == {
        'nil': None, 'one': {'a': 'b', 'c': [1, 2, 3]}, 'two': 2, 'three': '123', 'four': ['ab', 1, False]
    }


async def test_arrays():
    conn = await postgres_gateway.connect(POSTGRES_DSN)
    row = await conn.fetchrow("""
        SELECT array[1, 2, 3] AS ints,
               array['a', 'b', 'c'] AS strings,
               array['{"a": 1}'::JSON, '{"b": 2}'::JSON] AS jsons,
               array['{"a": 1}'::JSONB, '{"b": 2}'::JSONB] AS jsonbs;
    """)
    assert dict(row) == {
        'ints': [1, 2, 3],
        'strings': ['a', 'b', 'c'],
        'jsons': [{'a': 1}, {'b': 2}],
        'jsonbs': [{'a': 1}, {'b': 2}],
    }
