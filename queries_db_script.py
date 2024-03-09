def query_1():
    """Finds top 10 movies with the highest budget.
    In case of tie-breakers choose arbitrarily.
    :return: query
    """
    query = """
    SELECT title, budget
    FROM movies
    ORDER BY budget DESC
    LIMIT 10;
    """
    return query

def query_2():
    """Finds top 5 movies with the highest vote average where vote count > 1000.
    In case of tie-breakers choose arbitrarily.
    :return: query
    """
    query = """
    SELECT title, vote_average, runtime, overview
    FROM movies
    WHERE vote_count > 1000
    ORDER BY vote_average DESC
    LIMIT 5;
    """
    return query

def query_3(min_runtime, max_runtime):
    """Finds all movies with runtime between min_runtime and max_runtime.
    :param min_runtime: minimum runtime in minutes
    :param max_runtime: maximum runtime in minutes
    :return: query, params
    """
    query = f"""
    SELECT title, runtime, overview
    FROM movies
    WHERE runtime BETWEEN %s AND %s
    ORDER BY runtime;
    """
    return query, (min_runtime, max_runtime)

def query_4():
    """Finds all the actors with the highest average vote_average of the movies they had acted in.
    :return: query
    """
    query = """
    SELECT name, avg_vote_average
    FROM actors_avg_vote_average
    WHERE avg_vote_average = (
        SELECT MAX(avg_vote_average)
        FROM actors_avg_vote_average
    )
    """
    return query

def query_5(genre):
    """Finds all movies with the given genre.
    :param genre: genre name
    :return: query, params
    """
    query = f"""
    SELECT title, runtime, overview
    FROM movies
    JOIN movies_genres ON movies.id = movies_genres.movie_id
    JOIN genres ON movies_genres.genre_id = genres.id
    WHERE genres.name = %s
    """
    return query, (genre,)

def query_6(overview_search_string):
    """Finds movies that their overview contains words from overview_substring and order them by vote_average.
    :param overview_search_string: words to search in the overview
    :return: query, params
    """
    query = f"""
    SELECT title, vote_average, runtime, overview
    FROM movies
    WHERE MATCH(overview) AGAINST(%s IN NATURAL LANGUAGE MODE)
    ORDER BY vote_average DESC 
    """
    return query, (overview_search_string,)

def query_7(title_search_string):
    """Fins movies with the given substring in the title.
    :param title_search_string: words to search in the title
    :return: query, params
    """
    query = f"""
    SELECT title, runtime, overview
    FROM movies
    WHERE MATCH(title) AGAINST(%s IN NATURAL LANGUAGE MODE)
    """
    return query, (title_search_string,)

def query_8():
    """Finds top 10 countries with the largest number of movies produced in there.
    in case of tie-breakers choose arbitrarily.
    :return: query
    """
    query = """
    SELECT name, COUNT(movie_id) AS num_movies
    FROM production_countries
    JOIN movies_production_countries ON production_countries.iso_3166_1 = movies_production_countries.iso_3166_1
    GROUP BY name
    ORDER BY num_movies DESC
    LIMIT 10
    """
    return query

def query_9(production_country_name):
    """Finds all movies that have the maximal vote_average and were produced in the given country.
    :param production_country_name: country name
    :return: query, params
    """
    query = f"""
    SELECT title, vote_average
    FROM movies
    JOIN movies_production_countries ON movies.id = movies_production_countries.movie_id
    JOIN production_countries ON movies_production_countries.iso_3166_1 = production_countries.iso_3166_1
    WHERE production_countries.name = %s
    AND vote_average = (
        SELECT MAX(vote_average)
        FROM movies
        JOIN movies_production_countries ON movies.id = movies_production_countries.movie_id
        JOIN production_countries ON movies_production_countries.iso_3166_1 = production_countries.iso_3166_1
        WHERE production_countries.name = %s
    )
    """
    return query, (production_country_name, production_country_name)

def query_10():
    """Finds all genre names.
    :return: query
    """
    query = """
    SELECT DISTINCT name
    FROM genres
    """
    return query

def query_11():
    """Finds all countries name.
    :return: query
    """
    query = """
    SELECT DISTINCT name
    FROM production_countries
    """
    return query


