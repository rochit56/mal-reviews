import sqlite3
from config import DB
import pandas as pd


def run_query(q):
    with sqlite3.connect(DB) as conn:
        return pd.read_sql(q, conn)


def run_command(c):
    with sqlite3.connect(DB) as conn:
        conn.execute('PRAGMA foreign_key=ON;')
        conn.isolation_level = None
        conn.execute(c)


def run_inserts(DB, c, values):
    with sqlite3.connect(DB) as conn:
        conn.execute('PRAGMA foreign_key=ON;')
        conn.isolation_level = None
        conn.execute(c, values)

	
#Create the reviews table
c1 = """
CREATE TABLE reviews(
    review_id INTEGER PRIMARY KEY,
    anime_id INTEGER,
    review_date TEXT, 
    rating_tag TEXT,
    review_body TEXT,
    FOREIGN KEY(anime_id) REFERENCES animes(anime_id)  
); 
"""

run_command(c1)

