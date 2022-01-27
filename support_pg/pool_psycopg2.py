import psycopg2

from support_pg.util import DatabasePort


def main(database: DatabasePort):
    with psycopg2.connect(
            host="127.0.0.1",
            port=database.value,
            database="postgres",
            user="admin",
            password="quest",
    ) as connection:
        with connection.cursor() as cursor:
            try:
                if database == DatabasePort.QuestDB:
                    cursor.execute("drop table test;");
                elif database == DatabasePort.Postgres:
                    cursor.execute("drop table if exists test;");
            except:
                pass
            cursor.execute(
                """
                BEGIN;
                create table test(l int, s text);
                insert into test values(1, 'a');
                insert into test values(2, 'b');
                COMMIT;
                """
            )
            cursor.execute("select * from test;")
            data = cursor.fetchall()
            assert data[0] == (1, 'a')
            assert data[1] == (2, 'b')
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            assert "PostgreSQL" in str(version)


if __name__ == "__main__":
    main(DatabasePort.QuestDB)
