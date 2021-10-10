import pytest as pytest

import postgres_gateway

pytestmark = pytest.mark.asyncio


async def test_fetch_ints():
    conn = await postgres_gateway.connect('postgres://postgres:postgres@0.0.0.0:5432/postgres')
    row = await conn.fetchrow("""
        SELECT -8::int2 AS one, 2::int4 AS two, 3::int8 AS three;
    """)
    assert dict(row) == {'one': -8, 'two': 2, 'three': 3}
