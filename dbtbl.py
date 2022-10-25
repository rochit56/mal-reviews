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
