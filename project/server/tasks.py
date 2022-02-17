import os
import time
import csv
import json
import redis
import datetime

from celery import Celery
from celery.utils.log import get_task_logger

import random
import requests
from bs4 import BeautifulSoup
import pyuser_agent

celery = Celery(__name__)
celery.conf.broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379")
celery.conf.result_backend = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379")
logger = get_task_logger(__name__)

api_key = os.environ.get("SCRAPER_API_KEY")
db = redis.Redis(host='redis', port=6379, db=0)  # connect to server


def get_topic_page_count(topic):
    ua = pyuser_agent.UA()
    headers = {
        'User-Agent': ua.random,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
    }

    response = requests.get(f'https://github.com/topics/{topic}?page=1', headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    pages_f = int(
        soup.select_one('span[class="select-menu-item-text d-flex flex-justify-between"] span').text.strip().replace(
            ',',
            ''))

    if pages_f < 1020:
        pages = round(pages_f / 30)
    else:
        pages = 35

    if pages == 0:
        pages = 1

    return pages_f, pages


def get_all_projects(pages, _topic):
    _all_articles_frames = []

    for page in range(1, pages + 1):
        try:
            soup = get_soup_for_topic_projects(page, _topic)
            articles = soup.find_all('article', {'class': 'border rounded color-shadow-small color-bg-subtle my-4'})

            article_dict = {}
            _master_list = []
            for article in articles:
                _article_data = article.find('h3').findAll('a')

                article_dict['Name_of_org'] = _article_data[0].text.rstrip().lstrip().replace('\n',
                                                                                              '').rstrip().lstrip()
                article_dict['URL_of_org'] = 'https://github.com' + _article_data[0]['href'].rstrip().lstrip()

                article_dict['Name_of_project'] = _article_data[1].text.rstrip().lstrip().replace('\n',
                                                                                                  '').rstrip().lstrip()
                article_dict['URL_of_project'] = 'https://github.com' + _article_data[1]['href'].rstrip().lstrip()

                article_dict['Contributors_URL'] = article_dict['URL_of_project'] + '/graphs/contributors-data'

                _master_list.append(article_dict.copy())

            # REDIS CASH #
            field = f'{_topic}_{page}'
            value = json.dumps(_master_list)
            db.set(field, value, ex=7200)  # ttl 2 hours

            # add delay to the crawl. random is used to vary crawl delay
            time.sleep(random.randint(1, 3))

        except Exception as e:
            print('Error ', e)
            print(f'Error getting articles of Page {page} of {pages}')


def get_soup_for_topic_projects(page, _topic):
    ua = pyuser_agent.UA()
    headers = {
        'User-Agent': ua.random,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',

    }
    url = f'https://github.com/topics/{_topic}?page={page}'
    payload = {'api_key': api_key, 'url': url, 'keep_headers': 'true'}
    response = requests.get('http://api.scraperapi.com', params=payload, headers=headers)
    if response.status_code == 200:
        return BeautifulSoup(response.text, 'html.parser')
    else:

        return None


def get_data_for_contributors(url):
    global reTry_counter
    headers = {
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:96.0) Gecko/20100101 Firefox/96.0',

    }
    payload = {'api_key': api_key, 'url': url, 'keep_headers': 'true'}
    response = requests.get('http://api.scraperapi.com', params=payload, headers=headers)

    if response.status_code == 200:
        print('successful json data')
        reTry_counter = 0
        return response.json()

    else:
        print('Error in getting data for url: ', url, '\t', response.status_code, '<========')
        return None

    # else:
    #     print('Error in getting data for url: ', url, '\t', response.status_code, '<========')
    #     if reTry_counter < 10:
    #         time.sleep(3)
    #         reTry_counter = + 1
    #         return get_data_for_contributors(url)
    #
    #     else:
    #         return None


def get_contributors_for_topic_project(_project, _url):
    _contributors_data = get_data_for_contributors(_url)
    _contributors_list = []
    _final_contributors_dict = {'Project': _project}

    if _contributors_data is None:
        return None

    for _ in _contributors_data:
        _project_contributors_dict = {
            'Contributor': _['author']['login'],
            'Contributor_Account_URL': 'https://github.com' + _['author']['path']
        }

        _contributors_list.append(_project_contributors_dict.copy())

    _final_contributors_dict['Contributors'] = _contributors_list

    return _final_contributors_dict


def get_all_articles(pages, _topic):
    _all_articles_frames = []

    for page in range(1, pages + 1):
        try:
            time.sleep(random.randint(1, 3))
            soup = get_soup_for_topic_projects(page, _topic)
            if soup is not None:
                _temp_articles = get_articles_for_topic_projects_per_page(soup)
                _all_articles_frames.append(_temp_articles)
            else:
                print(f'Error getting articles of Page {page} of {pages}')

        except Exception as e:
            print('Error ', e)
            print(f'Error getting articles of Page {page} of {pages}')
            return _all_articles_frames

    return _all_articles_frames


def build_file(topic):
    absolute_path = os.path.dirname(os.path.abspath(__file__))

    timestamp = datetime.datetime.now().timestamp()
    filename = absolute_path + f'/reports/github_topic_contributors_{topic}_{int(round(timestamp))}.csv'

    headers = ['Name of topic', 'Name of org', 'Name of project', 'Url of project', 'Name of contributor',
               'Url of contributor']

    with open(filename, 'a+', newline='') as f:
        write = csv.writer(f)
        write.writerow(headers)

        keys = f'{topic}_*'
        all_keys = db.keys(keys)

        # each iteration is a page - update spreadsheet with each page
        for k in all_keys:
            master_matched_list_r = []
            value = db.get(k)
            j = json.loads(value)

            # iterate through each page - each iteration is a project
            for _project in j:
                _contributors_url = _project["Contributors_URL"]
                _contributors_dict = get_contributors_for_topic_project(_project, _contributors_url)

                if _contributors_dict is not None:
                    # loop through contributors
                    for _contributor in _contributors_dict["Contributors"]:
                        temp_ = [
                            topic,
                            _project["Name_of_org"],
                            _project["Name_of_project"],
                            _project["URL_of_project"],
                            _contributor["Contributor"],
                            _contributor["Contributor_Account_URL"]
                        ]
                        master_matched_list_r.append(temp_)

                # add delay to the crawl. random is used to vary crawl delay
                time.sleep(random.randint(1, 2))
            # write all contributors to a project to spreadsheet
            write.writerows(master_matched_list_r)


@celery.task(name="create_task")
def create_task(topic):
    pages_f, pages = get_topic_page_count(topic)
    mes = 'Total Listings Found ' + str(pages_f) + '; Pages ' + str(pages)
    logger.info(mes)

    # build projects and add to redis cache
    get_all_projects(pages, topic)

    # get contributors and add project to spreadsheet
    build_file(topic)

    return "Report Ready"
