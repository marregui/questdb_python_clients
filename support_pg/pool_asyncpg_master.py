import asyncio
import uuid
from enum import Enum

import asyncpg


class DatabasePort(Enum):
    QuestDB = 8812
    Postgres = 5432


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
    table_name = str(uuid.uuid4()())
    print(f"table_name: {table_name}")
    async with pool.acquire() as connection:
        # try:
        #     if database == DatabasePort.QuestDB:
        #         await connection.execute("drop table test;")
        #     elif database == DatabasePort.Postgres:
        #         await connection.execute("drop table if exists test;")
        # except:
        #     pass
        await connection.execute(f"create table {table_name}(l int, s text)")
        await connection.execute(f"insert into {table_name} values(1, 'a')")
        await connection.execute(f"insert into {table_name} values(2, 'b')")
        # await connection.execute("select * from test;")
        data = await connection.fetch(f"select * from {table_name}")
        print(data)
        assert data[0] == (1, 'a')
        assert data[1] == (2, 'b')
        version = await connection.fetchrow("SELECT version();")
        print(version)
        assert "PostgreSQL" in str(version)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main(DatabasePort.QuestDB))
