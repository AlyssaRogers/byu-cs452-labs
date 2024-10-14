## This script is used to create the tables in the database

import os
#from dotenv import load_dotenv
import psycopg2


def select_all_from_podcast(conn):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM podcast")

    rows = cur.fetchall()

    for row in rows:
        print(row)

#load_dotenv()

CONNECTION = 'a';


# need to run this to enable vector data type
CREATE_EXTENSION = "CREATE EXTENSION vector"

# TODO: Add create table statement
CREATE_PODCAST_TABLE = """
    DROP TABLE IF EXISTS podcast_segment;
    DROP TABLE IF EXISTS podcast;
    CREATE TABLE podcast (
        PK_id INT PRIMARY KEY,
        title TEXT
    );
"""
# TODO: Add create table statement
CREATE_SEGMENT_TABLE = """
    CREATE TABLE podcast_segment (
        PK_id INT PRIMARY KEY,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        content TEXT,
        embedding TEXT,
        podcast_id INT,
        FOREIGN KEY (podcast_id) references podcast(PK_id) 
    );
"""

conn = psycopg2.connect(CONNECTION)
# TODO: Create tables with psycopg2 (example: https://www.geeksforgeeks.org/executing-sql-query-with-psycopg2-in-python/)
conn.autocommit = True
cursor = conn.cursor() 

cursor.execute(CREATE_PODCAST_TABLE) 
cursor.execute(CREATE_SEGMENT_TABLE)
  
conn.commit() 
conn.close() 
