import sqlite3,os

# connect to the database
conn = sqlite3.connect('cache/db.sqlite3')

# read scripts from the flyway directory and create a sqlite database
for file in os.listdir('flyway'):
    with open('flyway', 'r') as f:
        conn.executescript(f.read())

# close the connection
conn.close()