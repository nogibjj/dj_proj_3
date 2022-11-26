import sqlite3
import csv
import pandas as pd
from sqlite3 import Error


def create_connection(db_file):
    """create a database connection to a SQLite database in Python"""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print("database connection created!")
    except Error as e:
        print(e)
    return conn


def create_table(c, create_table_sql):
    """create a table from the create_table_sql statement"""
    try:
        c.execute(create_table_sql)
    except Error as e:
        print(e)


def add_data_to_db(cur, filename, db_name, num_cols):
    """add data from csv file to database"""
    file = open(filename, encoding="utf8")
    contents = csv.reader(file)
    questionmarks = ", ".join("?" * num_cols)
    insert_records = "INSERT INTO " + db_name + " VALUES " + "(" + questionmarks + ")"
    cur.executemany(insert_records, contents)


if __name__ == "__main__":
    db_connection = create_connection(r"./sqlite/db/database2.db")
    cursor = db_connection.cursor()
    create_table(
        cursor,
        """CREATE TABLE IF NOT EXISTS sings (
                                artist TEXT,
                                song TEXT,
                                duration_ms INTEGER,
                                explicit BOOL,
                                year INTEGER,
                                popularity FLOAT,
                                danceability FLOAT,
                                energy FLOAT,
                                key FLOAT,
                                loudness FLOAT,
                                mode FLOAT,
                                speechiness FLOAT,
                                acousticness FLOAT,
                                instrumentalness FLOAT,
                                liveness FLOAT,
                                valence FLOAT,
                                tempo FLOAT,
                                genre TEXT
                                );""",
    )
    
    
    cursor.execute(
        "DELETE FROM sings;",
    )
    add_data_to_db(cursor, "spotify_data.csv", "sings", 18)


    # store it in a pandas dataframe
    surveys_df = pd.read_sql_query("SELECT * from sings WHERE explicit = 'TRUE'", db_connection)
    print(surveys_df)

    db_connection.commit()
    db_connection.close()