# The script is executed without error when the module is correctly implemented
import postgres_gateway
import asyncio


async def main():
    conn = await postgres_gateway.connect('postgres://postgres:postgres@0.0.0.0:5432/postgres')
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS abc(
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name TEXT
        );
    """)
    affected = await conn.execute("INSERT INTO abc (name) VALUES ('789');")
    assert affected == 1
    print('success')


if __name__ == '__main__':
    asyncio.run(main())
