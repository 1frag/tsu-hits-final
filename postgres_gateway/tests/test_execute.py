import pytest as pytest

import postgres_gateway
from constants import POSTGRES_DSN

pytestmark = pytest.mark.asyncio


async def test_execute():
    conn = await postgres_gateway.connect(POSTGRES_DSN)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS abc(
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name TEXT
        );
    """)
    affected = await conn.execute("INSERT INTO abc (name) VALUES ('789');")
    assert affected == 1
