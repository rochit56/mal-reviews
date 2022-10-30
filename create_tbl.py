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


# Create the reviews table
c1 = """
CREATE TABLE reviews(
    review_id INTEGER PRIMARY KEY,
    anime_id INTEGER,
    review_date TEXT, 
    reviewer_rating INT,
    review_tag TEXT,
    review_body TEXT,
    FOREIGN KEY(anime_id) REFERENCES animes(anime_id)  
); 
"""

run_command(c1)

# Create the animes table
c2 = """
CREATE TABLE animes(
    anime_id INTEGER PRIMARY KEY,
    anime_name TEXT,
    studio_id INT, 
    episodes_total TEXT,
    source_material TEXT,
    air_date TEXT,
    overall_rating FLOAT,
    members INT,
    synopsis TEXT,
    FOREIGN KEY(studio_id) REFERENCES studios(studio_id)  
); 
"""

run_command(c2)

# Create the tags table
c3 = """
CREATE TABLE tags(
    tag_id INTEGER PRIMARY KEY,
    tag_name TEXT
); 
"""

run_command(c3)

# Create the anime_tags table
c4 = """
CREATE TABLE anime_tags(
    anime_id INTEGER,
    tag_id INTEGER,
    PRIMARY KEY(anime_id, tag_id)
    FOREIGN KEY(anime_id) REFERENCES animes(anime_id),
    FOREIGN KEY(tag_id) REFERENCES tags(tag_id)   
); 
"""

run_command(c4)

# Create the studios table
c5 = """
CREATE TABLE studios(
    studio_id INTEGER PRIMARY KEY,
    studio_name TEXT  
); 
"""

run_command(c5)
