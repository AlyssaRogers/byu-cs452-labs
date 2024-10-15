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

CONNECTION = 'postgres://tsdbadmin:kwmtav03ndd501ym@mqeecwb2xv.l2ecxhvt0m.tsdb.cloud.timescale.com:39779/tsdb?sslmode=require';


# need to run this to enable vector data type
CREATE_EXTENSION = "CREATE EXTENSION vector"

# TODO: Add create table statement
CREATE_PODCAST_TABLE = """
    DROP TABLE IF EXISTS podcast_segment;
    DROP TABLE IF EXISTS podcast;
    CREATE TABLE podcast (
        id TEXT PRIMARY KEY,
        title TEXT
    );
"""
# TODO: Add create table statement
CREATE_SEGMENT_TABLE = """
    CREATE TABLE podcast_segment (
        id TEXT PRIMARY KEY,
        start_time DECIMAL(10, 2),
        end_time DECIMAL(10, 2),
        content TEXT,
        embedding TEXT,
        podcast_id TEXT,
        FOREIGN KEY (podcast_id) references podcast(id) 
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
