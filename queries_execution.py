from queries_db_script import *
from create_db_script import *
from drop_tables import *  # TODO delete
from api_data_retrieve import *
import mysql.connector

# TODO : add assumptions about parameters to the functions
# TODO : python versions and requirements
# TODO : explain why the first 3 rows are commented out according to the instructions of daniel in the forum
connection = mysql.connector.connect(
    host="127.0.0.1",
    port=3305,
    user="saradorfman",
    password="sar2885",
    database="saradorfman"
)
cursor = connection.cursor()

def running_query_1():
    cursor.execute(query_1())
    results = cursor.fetchall()
    print_results(results)

def running_query_2():
    cursor.execute(query_2())
    results = cursor.fetchall()
    print_results(results)

def running_query_3(min_runtime, max_runtime):
    cursor.execute(*query_3(min_runtime, max_runtime))
    results = cursor.fetchall()
    print_results(results)

def running_query_4():
    cursor.execute(query_4())
    results = cursor.fetchall()
    print_results(results)

def running_query_5(genre):
    cursor.execute(*query_5(genre))
    results = cursor.fetchall()
    print_results(results)

def running_query_6(overview_search_string):
    cursor.execute(*query_6(overview_search_string))
    results = cursor.fetchall()
    print_results(results)

def running_query_7(title_search_string):
    cursor.execute(*query_7(title_search_string))
    results = cursor.fetchall()
    print_results(results)

def running_query_8():
    cursor.execute(query_8())
    results = cursor.fetchall()
    print_results(results)

def running_query_9(production_country_name):
    cursor.execute(*query_9(production_country_name))
    results = cursor.fetchall()
    print_results(results)

def running_query_10():
    cursor.execute(query_10())
    results = cursor.fetchall()
    print_results(results)

def running_query_11():
    cursor.execute(query_11())
    results = cursor.fetchall()
    print_results(results)

def print_results(results):
    for row in results:
        print(row)


if __name__ == "__main__":
    drop_tables()  # TODO delete
    create_db(connection, cursor)
    retreive_data(connection, cursor)

    # run query 1 and print the results
    running_query_1()

    # run query 2 and print the results
    running_query_2()

    # run query 3 and print the results
    running_query_3(-2, 0)
    running_query_3(90, 110)

    # run query 4 and print the results
    running_query_4()

    # run query 5 and print the results
    running_query_5("Action")
    running_query_5("Comedy")
    running_query_5("Horror")

    # run query 6 and print the results
    running_query_6("story")
    running_query_6("love")
    running_query_6("war")

    # run query 7 and print the results
    running_query_7("ice")
    running_query_7("toy")
    running_query_7("hero")

    # run query 8 and print the results
    running_query_8()

    # run query 9 and print the results
    running_query_9("United States of America")
    running_query_9("United Kingdom")
    running_query_9("Germany")

    # run query 10 and print the results
    running_query_10()

    # run query 11 and print the results
    running_query_11()

    cursor.close()
    connection.close()
