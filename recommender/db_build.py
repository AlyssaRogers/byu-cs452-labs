## This script is used to create the tables in the database

import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

CONNECTION = None # paste connection string here or read from .env file

# need to run this to enable vector data type
CREATE_EXTENSION = "CREATE EXTENSION vector"

# TODO: Add create table statement
CREATE_PODCAST_TABLE = """
    CREATE TABLE podcast (
        PK_id INT PRIMARY KEY ,
        title TEXT,
        FOREIGN KEY (podcast_segment) references podcast_segment(pk_id)
    );
"""
# TODO: Add create table statement
CREATE_SEGMENT_TABLE = """
    CREATE TABLE podcast_segment (
        PK_id INT PRIMARY KEY ,
        start_time TIMESTAMP,
        content TEXT,
        embedding TEXT,
        FK: podcast_id
        FOREIGN KEY (FK) references podcast(pk_id)       
    );
"""

conn = psycopg2.connect(CONNECTION)
# TODO: Create tables with psycopg2 (example: https://www.geeksforgeeks.org/executing-sql-query-with-psycopg2-in-python/)
conn.autocommit = True
cursor = conn.cursor() 
  
sql = '''CREATE TABLE employees(emp_id int,emp_name varchar, \ 
salary decimal); '''
  
cursor.execute(CREATE_PODCAST_TABLE) 
cursor.execute(CREATE_SEGMENT_TABLE)
  
conn.commit() 
conn.close() 

