import random
import requests
from bs4 import BeautifulSoup
import pyuser_agent


class TopicData:
    def __init__(self, topic):
        self.topic = topic

    def get_topic_page_count(self):
        ua = pyuser_agent.UA()
        headers = {
          'User-Agent': ua.random,
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.5',
          'Accept-Encoding': 'gzip, deflate, br',
          'Connection': 'keep-alive',
        }

        response = requests.get(f'https://github.com/topics/{self.topic}?page=1', headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')

        try:
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
        except Exception as e:
            print('Nothing found')
            return 0, 0
