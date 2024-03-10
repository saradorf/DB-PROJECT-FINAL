import mysql.connector
from mysql.connector import errorcode

def create_db(connection, cursor):
    TABLES = {}

    TABLES['movies'] = (
        "CREATE TABLE `movies` ("
        "  `id` int(11) NOT NULL,"
        "  `title` varchar(255) NOT NULL,"
        "  `overview` TEXT,"
        "  `runtime` int(11) NOT NULL,"
        "  `budget` float NOT NULL,"
        "  `vote_average` float NOT NULL,"
        "  `vote_count` int(11) NOT NULL,"
        "   PRIMARY KEY (`id`),"
        "   INDEX `vote_count_index` (`vote_count`),"
        "   INDEX `runtime_index` (`runtime`),"
        "   INDEX `budget_index` (`budget`),"
        "   FULLTEXT (`overview`),"
        "   FULLTEXT (`title`)"
        ") ENGINE=InnoDB")

    TABLES['actors'] = (
        "CREATE TABLE `actors` ("
        "  `id` int(11) NOT NULL,"
        "  `name` varchar(255) NOT NULL,"
        "   PRIMARY KEY (`id`)"
        ") ENGINE=InnoDB")

    TABLES['movies_actors'] = (
        "CREATE TABLE `movies_actors` ("
        "  `movie_id` int(11) NOT NULL,"
        "  `actor_id` int(11) NOT NULL,"
        "   PRIMARY KEY (`movie_id`, `actor_id`),"
        "   FOREIGN KEY (`movie_id`) REFERENCES `movies` (`id`),"
        "   FOREIGN KEY (`actor_id`) REFERENCES `actors` (`id`)"
        ") ENGINE=InnoDB")


    TABLES['genres'] = (
        "CREATE TABLE `genres` ("
        "  `id` int(11) NOT NULL,"
        "  `name` varchar(255) NOT NULL,"
        "   INDEX `genre_name_index` (`name`),"
        "   PRIMARY KEY (`id`)"
        ") ENGINE=InnoDB")


    TABLES['movies_genres'] = (
        "CREATE TABLE `movies_genres` ("
        "  `movie_id` int(11) NOT NULL,"
        "  `genre_id` int(11) NOT NULL,"
        "   PRIMARY KEY (`movie_id`, `genre_id`),"
        "   FOREIGN KEY (`movie_id`) REFERENCES `movies` (`id`),"
        "   FOREIGN KEY (`genre_id`) REFERENCES `genres` (`id`)"
        ") ENGINE=InnoDB")

    TABLES['production_countries'] = (
        "CREATE TABLE `production_countries` ("
        "  `iso_3166_1` char(2) NOT NULL,"
        "  `name` varchar(255) NOT NULL,"
        "   PRIMARY KEY (`iso_3166_1`),"
        "   INDEX `country_name_index` (`name`)"
        ") ENGINE=InnoDB")

    TABLES['movies_production_countries'] = (
        "CREATE TABLE `movies_production_countries` ("
        "  `movie_id` int(11) NOT NULL,"
        "  `iso_3166_1` char(2) NOT NULL,"
        "   PRIMARY KEY (`movie_id`, `iso_3166_1`),"
        "   FOREIGN KEY (`movie_id`) REFERENCES `movies` (`id`),"
        "   FOREIGN KEY (`iso_3166_1`) REFERENCES `production_countries` (`iso_3166_1`)"
        ") ENGINE=InnoDB")

    # Notice that we are creating a view here
    # This a view for retrieving the average vote average for each actor regarding the movies he played in
    TABLES['actors_avg_vote_average'] = (
        "CREATE VIEW `actors_avg_vote_average` AS "
        "SELECT actors.id, actors.name, AVG(movies.vote_average) AS avg_vote_average "
        "FROM actors "
        "JOIN movies_actors ON actors.id = movies_actors.actor_id "
        "JOIN movies ON movies_actors.movie_id = movies.id "
        "GROUP BY actors.id, actors.name"
    )

    for table_name in TABLES:
        table_description = TABLES[table_name]
        try:
            print(f"Creating {table_name}: ", end='')
            cursor.execute(table_description)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
                connection.rollback()
                return err.errno
        else:
            print("OK")

    connection.commit()
    return 0


