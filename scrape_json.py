import requests
import pickle
import datetime
import time
import csv
import os
import math
import multiprocessing as mp

class scraper:
    """

    """
    def __init__(self, data_name, url, params=None):
        self.name = data_name
        self.url = url
        self.params = params
        self.current_data = {}

    def scrape(self, interval_seconds, duration_minutes):
        duration_seconds = duration_minutes * 60
        num_requests = math.ceil(duration_seconds / interval_seconds)
        for i in range(num_requests):
            r = requests.get(self.url, params=self.params)
            if r.status_code == 200:
                self.current_data[datetime.datetime.now()] = r.json()
            else:
                self.current_data[datetime.datetime.now()] = None
            time.sleep(interval_seconds)
        return

    def write(self, directory):
        fn = datetime.datetime.now().strftime('{}_%m%d%Y-%H%M%S.pkl'.format(self.name))
        with open(os.path.join(directory, fn), 'wb') as cf:
            pickle.dump(self.current_data, cf, protocol=4)
        self.current_data = {}
        return


def run_scraper(scraper_name, scraper_url):
    s = scraper(data_name=scraper_name, url=scraper_url)
    while True:
        s.scrape(interval_seconds=60, duration_minutes=60)
        s.write(directory='data/')


if __name__ == '__main__':
    # load up list of URL's
    with open('urls.txt', 'r') as f:
        reader = csv.reader(f, delimiter=';')
        urls = list(reader)
    # initialize data directory at ./data
    if 'data' not in os.listdir('./'):
        os.mkdir('./data')

    p = mp.Pool(len(urls))
    p.starmap(run_scraper, urls)

