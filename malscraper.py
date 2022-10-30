from config import *
from requests import get
from bs4 import BeautifulSoup
import os
import re
import math
import random
import time
import pandas as pd
import sqlite3


# interface with SQL
def run_query(DB, q):
    with sqlite3.connect(DB) as conn:
        return pd.read_sql(q, conn)


def run_cmd(DB, c):
    with sqlite3.connect(DB) as conn:
        conn.execute('PRAGMA foreign_key = ON;')
        conn.isolation_level = None
        conn.execute(c)


def run_insert(DB, c, val):
    with sqlite3.connect(DB) as conn:
        conn.execute('PRAGMA foreign_keys = ON;')
        conn.isolation_level = None
        conn.execute(c, val)


# Studios Scrapper
def studios_scrape(DB=DB):
    start_time = time.time()
    insert_q = '''
    INSERT OR IGNORE INTO studios(
        studio_id,
        studio_name
    )
    VALUES (?,?)
    '''

    # special query for unknown studio
    insert_sp = '''
    INSERT OR IGNORE INTO studios(
        studio_id,
        studio_name
    )
    VALUES (9999,'Unknown')
    '''

    run_cmd(DB, insert_sp)

    # make request
    url = 'https://myanimelist.net/anime/producer'
    headers = {
        "Usr-agent": "mal scrapper for research"
    }

    # handel timeouts
    try:
        response = get(url, headers=headers, timeout=10)
    except:
        print("request timeout")

    # dump failed query
    failed_q = []

    # create soup object
    html_soup = BeautifulSoup(response.text, 'html.parser')
    total_studios = len(html_soup.find_all('a', class_='genre-name-link'))
    for i in range(total_studios):
        result = html_soup.find_all(
            'a', class_='genre-name-link')[i].attrs['href'].replace('/anime/producer/', '').split('/', 1)
        studio_id = result[0]
        studio_name = result[1]

        # Write into SQL DB
        try:
            run_insert(DB, insert_q, (
                int(studio_id),
                studio_name
            ))
        except:
            print('Insert failed {}'.format(studio_name))
            failed_q.append(studio_name)
            pass

        # provide stats
        os.system('cls' if os.name == 'nt' else 'clear')
        print('scraping studio data ')
        print('scraping {}'.format(url))
        print('inserted into database: \'{}\''.format(studio_name))

    print('scraping complete')
    print('time elapsed : {:.4f} s'.format(time.time()-start_time))


# Tags Scrapper
def tags_scrape(DB=DB):
    start_time = time.time()
    insert_q = '''
    INSERT OR IGNORE INTO tags(
        tag_id,
        tag_name
    )
    VALUES (?,?)
    '''

    # make request
    url = 'https://myanimelist.net/anime.php'
    headers = {
        "Usr-agent": "mal scrapper for research"
    }

    # handel timeouts
    try:
        response = get(url, headers=headers, timeout=10)
    except:
        print("request timeout")

    # dump failed query
    failed_q = []

    # create soup object
    html_soup = BeautifulSoup(response.text, 'html.parser')
    total_tags = len(html_soup.find_all('a', class_='genre-name-link'))
    for i in range(total_tags):
        res = html_soup.find_all(
            'a', class_='genre-name-link')[i].attrs['href']
        # check for links
        x = re.search("^/anime/genre", res)
        if x:
            result = html_soup.find_all(
                'a', class_='genre-name-link')[i].attrs['href'].replace('/anime/genre/', '').split('/', 1)
            tag_id = result[0]
            tag_name = result[1]

            # Write in to SQL DB
            try:
                run_insert(DB, insert_q, (
                    int(tag_id),
                    tag_name
                ))
            except:
                print('Insert failed {}'.format(tag_name))
                failed_q.append(tag_name)
                pass

            # provide stats
            os.system('cls' if os.name == 'nt' else 'clear')
            print('scraping studio data ')
            print('scraping {}'.format(url))
            print('inserted into database: \'{}\''.format(tag_name))

    print('scraping complete')
    print('time elapsed : {:.4f} s'.format(time.time()-start_time))


def scrape_all():
    studios_scrape()
    tags_scrape()
    print('Halting...')
    time.sleep(random.uniform(sleep_min,sleep_max))
    os.system('cls' if os.name=='nt' else 'clear')
