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


# Studios Scraper
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
        "Usr-agent": "mal scraper for research"
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


# Tags Scraper
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
        "Usr-agent": "mal scraper for research"
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
        if re.search("^/anime/genre", res):
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


def anime_scrape(DB=DB, sleep_min=sleep_min, sleep_max=sleep_max):
    start_time = time.time()
    insert_q1 = '''
    INSERT OR IGNORE INTO animes(
        anime_id,
        anime_name,
        studio_id,
        episodes_total,
        source_material,
        air_date,
        overall_rating,
        members,
        synopsis
    ) 
    VALUES(?,?,?,?,?,?,?,?,?)
    '''

    insert_q2 = '''
    INSERT OR IGNORE INTO anime_tags(
        anime_id,
        tag_id
    )
    VALUES (?,?)
    '''

    # make request
    url = 'https://myanimelist.net/anime.php'
    headers = {
        "Usr-agent": "mal scraper for research"
    }

    # handel timeouts
    try:
        response = get(url, headers=headers, timeout=10)
    except:
        print("request timeout")

    # dump failed query
    failed_q = []

    # create soup object
    html_soup_initial = BeautifulSoup(response.text, 'html.parser')
    total_tags = html_soup_initial.find_all('a', class_='genre-name-link')

    requests = 0
    for i in range(len(total_tags)):
        tag_details = total_tags[i]
        res = total_tags[i].attrs['href']
        if re.search("^/anime/genre", res):
            total_animes = int(tag_details.text.split(
                '(')[-1].replace(')', '').replace(',', ''))
            link_val = res.replace('/anime/genre/', '').split('/')[0]

            for i in range(math.ceil(total_animes/100)):
                url = 'https://myanimelist.net/anime/genre/{0}/?page={1}'.format(
                    link_val, i+1)
                headers = {
                    "Usr-agent": "mal scraper for research"
                }
                print("Scraping {}".format(url))

                # handel timeouts
                try:
                    response = get(url, headers=headers, timeout=10)
                except:
                    print("Request timeout")
                    pass

                if response.status_code != 200:
                    print("Request: {}; Status code: {}".format(
                        requests, response.status_code))
                    pass

                html_soup = BeautifulSoup(response.text, 'html.parser')
                containers = html_soup.find_all('div', class_='seasonal-anime')
                for container in containers:

                    # primary key for "animes"
                    anime_id = container.find(
                        'div', class_='genres js-genre').attrs['id']

                    # foreign key for "animes"
                    for r in container.find_all('a'):
                        x = re.search("^/anime/producer/", r.attrs['href'])
                        if x:
                            try:
                                studio_id = r.attrs['href'].replace(
                                    '/anime/producer/', '').split('/')[0]
                            except:
                                studio_id = 9999

                    # Anime info
                    anime_name = container.find('a', class_='link-title').text
                    for r in container.find('div', class_='info').find_all('span'):
                        if re.search("eps$|ep$", r.text):
                            ep_totals = r.text
                            break
                    for r in container.find_all('div', class_='property'):
                        if re.search("^Source", r.find('span', class_='caption').text):
                            src_mat = r.find('span', class_='item').text
                            break
                    for r in container.find('div', class_='info').find_all('span', class_='item'):
                        if re.search("[0-9]$", r.text):
                            air_dt = int(r.text.split(',')[1])
                            break
                    member = container.find_all(
                        'div', class_='scormem-item')[1].text
                    synopsis = container.find(
                        'div', class_='synopsis js-synopsis').find('p', class_='preline').text
                    try:
                        orating = float(container.find_all(
                            'div', class_='scormem-item')[0].text)
                    except:
                        orating = 0.0

                    # write into SQL DB
                    try:
                        run_insert(DB, insert_q1(int(anime_id), anime_name, int(studio_id),
                                   ep_totals, src_mat, air_dt, orating, member, synopsis))
                    except Exception as e:
                        print('Failed to insert into animes for anime_id: {0}, {1}'.format(
                            anime_id, e))
                        pass

                    # Container for anime_tags
                    anime_tags = []
                    for r in container.find('div', class_='properties').find_all('a'):
                        if re.search("^/anime/genre", r.attrs['href']):
                            anime_tags.append(r)
                    for tag in anime_tags:
                        tag_id = tag.attrs['href'].replace(
                            '/anime/genre/', '').split('/')[0]
                        for t in tag_id:
                            try:
                                run_insert(DB, insert_q2(
                                    int(anime_id), int(t)))
                            except Exception as e:
                                print('Failed to insert into anime_tags for anime_id: {0}, {1}'.format(
                                    anime_id, e))
                                pass

                 # provide stats
                current_time = time.time()
                elapsed_time = current_time - start_time
                requests += 1

                print(
                    'Requests Completed: {}; Frequency: {} requests/s'.format(requests, requests/elapsed_time))
                print('Elapased Time: {} minutes'.format(elapsed_time/60))
                print('Pausing...')
                time.sleep(random.uniform(sleep_min, sleep_max))


def scrape_all():
    studios_scrape()
    tags_scrape()
    print('Halting...')
    time.sleep(random.uniform(sleep_min, sleep_max))
    os.system('cls' if os.name == 'nt' else 'clear')
    anime_scrape()


scrape_all()
