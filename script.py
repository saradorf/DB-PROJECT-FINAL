import mysql.connector

# Connect to the database
connection = mysql.connector.connect(
    host="127.0.0.1",
    port=3305,
    user="saradorfman",
    password="sar2885",
    database="saradorfman"
)

connection.close()
