import uuid

import pytest as pytest

import postgres_gateway
from constants import POSTGRES_DSN

pytestmark = pytest.mark.asyncio


async def test_unique():
    conn = await postgres_gateway.connect(POSTGRES_DSN)
    await conn.execute("""
        DROP TABLE IF EXISTS abc;
        CREATE TABLE IF NOT EXISTS abc (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name TEXT UNIQUE
        );
    """)
    id_ = uuid.uuid4()
    ok = await conn.execute(f"INSERT INTO abc (id) VALUES ('{id_}');")
    with pytest.raises(postgres_gateway.UniqueViolationError):
        await conn.execute(f"INSERT INTO abc (id) VALUES ('{id_}');")
    # assert affected == 1
