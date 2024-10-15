## This script is used to query the database
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

CONNECTION = "postgresql://tsdbadmin:m0s75x3snu9h65mm@xj0qo65uj8.ahdljd7112.tsdb.cloud.timescale.com:31025/tsdb?sslmode=require"

Q1 = """
    SELECT content
    FROM podcast_segment
    WHERE content LIKE CONCAT('%', (SELECT content FROM podcast_segment WHERE id = '267:476'), '%')
    LIMIT 5;
"""

Q2 = """
    SELECT content
    FROM podcast_segment
    WHERE content LIKE CONCAT('%', (SELECT content FROM podcast_segment WHERE id = '267:476' ORDER BY content DESC), '%')
    LIMIT 5;
"""

Q3 = """
    SELECT content
    FROM podcast_segment
    WHERE content LIKE CONCAT('%', (SELECT content FROM podcast_segment WHERE id = '48:511'), '%')
    LIMIT 5;
"""

Q4 = """
    SELECT content
    FROM podcast_segment
    WHERE content LIKE CONCAT('%', (SELECT content FROM podcast_segment WHERE id = '51:56'), '%')
    LIMIT 5;
""" 

Q5a = """
    SELECT * 
    FROM podcast_segment
    WHERE content LIKE CONCAT('%', (SELECT id, AVG(embedding) FROM podcast_segment GROUP BY id), '%;)
    ORDER BY embedding <-> "267:476"
    LIMIT 5;
"""
Q5b = """
    SELECT * 
    FROM podcast_segment
    WHERE content LIKE CONCAT('%', (SELECT id, AVG(embedding) FROM podcast_segment GROUP BY id), '%;)
    ORDER BY embedding <-> '48:511'
    LIMIT 5;
"""

Q5c = """
    SELECT * 
    FROM podcast_segment
    WHERE content LIKE CONCAT('%', (SELECT id, AVG(embedding) FROM podcast_segment GROUP BY id), '%;)
    ORDER BY embedding <-> '51:56'
    LIMIT 5;
"""

Q6 = """
SELECT * 
    FROM podcast_segment
    WHERE content LIKE CONCAT('%', (SELECT id, AVG(embedding) FROM podcast_segment WHERE id = VeH7qKZr0WI GROUP BY id ), '%;)
    ORDER BY embedding <-> '51:56'
    LIMIT 5;
"""
