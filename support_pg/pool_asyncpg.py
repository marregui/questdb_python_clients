import asyncio
import uuid

import asyncpg

from support_pg.util import DatabasePort


async def main(database: DatabasePort):
    pool = await asyncpg.create_pool(
        host="127.0.0.1",
        port=database.value,
        database="postgres",
        user="admin",
        password="quest",
        min_size=1,
        max_size=1,
    )
    table_name = str(uuid.uuid4())
    table_name = "pepe"
    async with pool.acquire() as connection:
        try:
            if database == DatabasePort.QuestDB:
                await connection.execute(f"drop table '{table_name}'")
            elif database == DatabasePort.Postgres:
                await connection.execute(f"drop table if exists '{table_name}'")
        except:
            pass
        await connection.execute(
            f"""
            BEGIN;
            create table '{table_name}'(l int, s text);
            insert into '{table_name}' values(1, 'a');
            insert into '{table_name}' values(2, 'b');
            COMMIT;
            """
        )
        # await connection.execute("select * from test;")
        await connection.execute(f"select * from '{table_name}'")
        data = await connection.fetch(f"select * from '{table_name}'")
        print(data)
        assert data[0] == (1, 'a')
        assert data[1] == (2, 'b')
        version = await connection.fetchrow("SELECT version();")
        print(version)
        assert "PostgreSQL" in str(version)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main(DatabasePort.QuestDB))
