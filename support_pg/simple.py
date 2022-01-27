import asyncio

import datetime

import asyncpg

from support_pg.util import DatabasePort

DATA = (
    ("1", "Alicia", datetime.date(2002, 5, 16)),
    ("2", "Berta", datetime.date(1954, 7, 29)),
    ("3", "Carla", datetime.date(1987, 4, 3))
)


async def main(database: DatabasePort, data: tuple):
    pool = await asyncpg.create_pool(
        host="127.0.0.1",
        port=database.value,
        database="postgres",
        user="admin",
        password="quest",
        min_size=1,
        max_size=1,
    )
    async with pool.acquire() as connection:
        await connection.set_type_codec(
            typename='date',
            encoder=str,
            decoder=str,
            schema='pg_catalog',
            format='text'
        )
        if database == DatabasePort.QuestDB:
            create_table_stmt = """
            CREATE TABLE IF NOT EXISTS foo_user(
                id symbol,
                name symbol,
                dob date,
                ts timestamp
            ) timestamp(ts) partition by year
            """
        elif database == DatabasePort.Postgres:
            create_table_stmt = """
            CREATE TABLE IF NOT EXISTS foo_user(
                id text,
                name text,
                dob date,
                ts timestamp
            )
            """
        await connection.execute(create_table_stmt)
        for data_row in data:
            if database == DatabasePort.QuestDB:
                await connection.execute(
                    "INSERT INTO foo_user(id, name, dob, ts) VALUES($1, $2, $3, $4)",
                    data_row[0], data_row[1], data_row[2], datetime.datetime.utcnow()
                )
            elif database == DatabasePort.Postgres:
                await connection.execute(
                    "INSERT INTO foo_user(id, name, dob, ts) VALUES($1, $2, $3, $4)",
                    data_row[0], data_row[1], data_row[2], datetime.datetime.utcnow()
                )
            row = await connection.fetchrow(
                'SELECT * FROM foo_user WHERE id = $1',
                data_row[0]
            )
            print(f"{data_row} -> {row}")
            assert row["id"] == data_row[0]
            assert row["name"] == data_row[1]
            if database == DatabasePort.QuestDB:
                assert row["dob"].date() == data_row[2]
            elif database == DatabasePort.Postgres:
                assert row["dob"] == str(data_row[2])

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main(DatabasePort.QuestDB, DATA))
