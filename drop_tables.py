import mysql.connector
from mysql.connector import errorcode

def drop_tables(connection, cursor):

    TABLES = {}

    TABLES['actors_avg_vote_average'] = (
        "DROP VIEW `actors_avg_vote_average`"
    )

    TABLES['movies_production_countries'] = (
        "DROP TABLE `movies_production_countries`"
    )

    TABLES['movies_actors'] = (
        "DROP TABLE `movies_actors`"
    )

    TABLES['movies_genres'] = (
        "DROP TABLE `movies_genres`"
    )

    TABLES['movies'] = (
        "DROP TABLE `movies`"
    )

    TABLES['actors'] = (
        "DROP TABLE `actors`"
    )

    TABLES['genres'] = (
        "DROP TABLE `genres`"
    )

    TABLES['production_countries'] = (
        "DROP TABLE `production_countries`"
    )

    for table_name in TABLES:
        table_description = TABLES[table_name]
        try:
            print(f"Dropping {table_name}...")
            cursor.execute(table_description)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_BAD_TABLE_ERROR:
                print(f"{table_name} doesn't exist")
            else:
                print(err.msg)
                connection.rollback()
                return err.errno
        else:
            print(f"{table_name} dropped")

    connection.commit()
    return 0
