import pytest as pytest

import postgres_gateway
from constants import POSTGRES_DSN

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
async def cleanup_db():
    conn = await postgres_gateway.connect(POSTGRES_DSN)
    for cmd in (
        'DROP SCHEMA public CASCADE;',
        'CREATE SCHEMA public;',
    ):
        await conn.execute(cmd)
