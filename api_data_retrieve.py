import pandas as pd
from pandasql import sqldf
import ast
import mysql.connector


def get_processed_data():
    # Sample data
    movies_table = pd.read_csv('movies_metadata.csv')
    credits_table = pd.read_csv('credits.csv')

    # Filter out movies with ids that are not integers
    movies_table = movies_table[movies_table['id'].apply(lambda x: x.isnumeric())]

    movies_table['id'] = movies_table['id'].astype('int64')
    credits_table['id'] = credits_table['id'].astype('int64')

    # Define the join query on the id column and make sure there is only one id column in the result
    query = """
    SELECT DISTINCT
        movies_table.id,
        title,
        overview,
        runtime,
        budget,
        `genres`,
        `production_countries`,
        vote_average,
        vote_count,
        `cast`
    FROM
        movies_table
    JOIN
        credits_table ON movies_table.id = credits_table.id
    WHERE
        movies_table.id IS NOT NULL
        AND title IS NOT NULL AND title != '' AND title REGEXP '[A-Za-z0-9_.,:{}"''\\[\\]/ ?!-]*'
        AND overview IS NOT NULL AND overview != '' AND overview REGEXP '[A-Za-z0-9_.,:{}"''\\[\\]/ ?!-]*'
        AND runtime IS NOT NULL AND runtime > 0
        AND budget IS NOT NULL
        AND vote_average IS NOT NULL
        AND vote_count IS NOT NULL
        AND `cast` IS NOT NULL AND `cast` != '' AND `cast` != '[]' AND `cast` REGEXP '[A-Za-z0-9_.,:{}"''\\[\\]/ ]*' AND `cast` LIKE '%}]'
        AND `genres` IS NOT NULL AND `genres` != '' AND `genres` != '[]' AND `genres` REGEXP '[A-Za-z0-9_.,:{}"''\\[\\]/ ]*' AND `genres` LIKE '%}]'
        AND `production_countries` IS NOT NULL AND `production_countries` != '' AND `production_countries` != '[]' AND `production_countries` REGEXP '[A-Za-z0-9_.,:{}"''\\[\\]/ ]*' AND `production_countries` LIKE '%}]'
    """

    # Execute the query
    result = sqldf(query, locals())
    # print number of rows and columns
    # print(result.shape)
    # print(result.columns)
    return result

def retrieve_data(connection, cursor):
    movies = get_processed_data()

    # ================ Create lists of tuples for the database ================
    actors = {}  # create dictionary for columns id, name for actors
    genres = {}  # create dictionary for columns id, name for genres
    production_countries = {}  # create dictionary for columns iso_3166_1, name for production_countries
    movies_table = []  # create list of tuples with columns id, title, overview, budget, vote_average, vote_count
    movies_actors = []  # create list of tuples with columns movie_id, actor_id
    movies_genres = []  # create list of tuples with columns movie_id, genre_id
    movies_production_countries = []  # create list of tuples with columns movie_id, iso_3166_1

    for _, row in movies.iterrows():
        movie_id = row['id']
        actors_in_row = ast.literal_eval(row['cast'])
        genres_in_row = ast.literal_eval(row['genres'])
        production_countries_in_row = ast.literal_eval(row['production_countries'])

        # append to movies_table
        movies_table.append((movie_id, row['title'], row['overview'], row['runtime'], row['budget'], row['vote_average'], row['vote_count']))

        for actor in actors_in_row:
            # append to actors dictionary
            if actor['id'] not in actors:
                actors[actor['id']] = actor['name']
            # append to movies_actors
            movies_actors.append((movie_id, actor['id']))

        for genre in genres_in_row:
            # append to genres dictionary
            if genre['id'] not in genres:
                genres[genre['id']] = genre['name']
            # append to movies_genres
            movies_genres.append((movie_id, genre['id']))

        for country in production_countries_in_row:
            # append to production_countries dictionary
            if country['iso_3166_1'] not in production_countries:
                production_countries[country['iso_3166_1']] = country['name']
            # append to movies_production_countries
            movies_production_countries.append((movie_id, country['iso_3166_1']))

    actors = list(actors.items())
    genres = list(genres.items())
    production_countries = list(production_countries.items())

    # ===============================================================

    # ================ Insert data into the database ================

    # Insert data into actors table
    sql_query = "INSERT INTO actors (id, name) VALUES (%s, %s)"
    try:
        cursor.executemany(sql_query, actors)
    except mysql.connector.Error as err:
        print(err.msg)
        connection.rollback()
        return err.errno

    # Insert data into genres table
    sql_query = "INSERT INTO genres (id, name) VALUES (%s, %s)"
    try:
        cursor.executemany(sql_query, genres)
    except mysql.connector.Error as err:
        print(err.msg)
        connection.rollback()
        return err.errno

    # Insert data into production_countries table
    sql_query = "INSERT INTO production_countries (iso_3166_1, name) VALUES (%s, %s)"
    try:
        cursor.executemany(sql_query, production_countries)
    except mysql.connector.Error as err:
        print(err.msg)
        connection.rollback()
        return err.errno

    # Insert data into movies table
    sql_query = "INSERT INTO movies (id, title, overview, runtime, budget, vote_average, vote_count) VALUES (%s, %s, %s, %s, %s, %s, %s) " \
                "ON DUPLICATE KEY UPDATE id=VALUES(id), title=VALUES(title), overview=VALUES(overview), runtime=VALUES(runtime), budget=VALUES(budget), vote_average=VALUES(vote_average), vote_count=VALUES(vote_count)"
    try:
        cursor.executemany(sql_query, movies_table)
    except mysql.connector.Error as err:
        print(err.msg)
        connection.rollback()
        return err.errno

    # Insert data into movies_actors table
    sql_query = "INSERT INTO movies_actors (movie_id, actor_id) VALUES (%s, %s)" \
                "ON DUPLICATE KEY UPDATE movie_id=VALUES(movie_id), actor_id=VALUES(actor_id)"
    try:
        cursor.executemany(sql_query, movies_actors)
    except mysql.connector.Error as err:
        print(err.msg)
        connection.rollback()
        return err.errno

    # Insert data into movies_genres table
    sql_query = "INSERT INTO movies_genres (movie_id, genre_id) VALUES (%s, %s)" \
                "ON DUPLICATE KEY UPDATE movie_id=VALUES(movie_id), genre_id=VALUES(genre_id)"
    try:
        cursor.executemany(sql_query, movies_genres)
    except mysql.connector.Error as err:
        print(err.msg)
        connection.rollback()
        return err.errno

    # Insert data into movies_production_countries table
    sql_query = "INSERT INTO movies_production_countries (movie_id, iso_3166_1) VALUES (%s, %s)" \
                "ON DUPLICATE KEY UPDATE movie_id=VALUES(movie_id), iso_3166_1=VALUES(iso_3166_1)"
    try:
        cursor.executemany(sql_query, movies_production_countries)
    except mysql.connector.Error as err:
        print(err.msg)
        connection.rollback()
        return err.errno

    connection.commit()
    return 0

    # ===============================================================
