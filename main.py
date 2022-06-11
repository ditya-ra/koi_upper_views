import os
import random
import re
import time
from datetime import datetime
from multiprocessing import Pool

import dotenv
from dateutil import parser
from loguru import logger
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from tqdm import tqdm

from services.service import Service
from settings import driver_settings

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

dotenv.load_dotenv(os.path.join(BASE_DIR, '.env'))

PROXY_PORT = os.environ['PROXY_PORT']
PROXY_USER = os.environ['PROXY_USER']
PROXY_PASSWORD = os.environ['PROXY_PASSWORD']

logger.add(f"{os.path.join(BASE_DIR, 'logger.log')}", format="{time} $$$ {level} $$$ {message}", level="INFO")

db_service = Service()


def nft_view_action(task):
    proxy, nft = task
    try:
        driver = driver_settings.driver_init(proxy_host=proxy.host, proxy_port=int(PROXY_PORT), proxy_user=PROXY_USER,
                                             proxy_password=PROXY_PASSWORD)
        driver.get(nft.url)
        time.sleep(random.randint(20, 30))

        views = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//*[text()[contains(.,'views')]]")), "Not Found Element"
        )

        views_amount = re.search('.+?(?=views)', views.text).group(0).strip()

        true_view = driver.find_elements(By.XPATH,
                                         "//*[text()[contains(.,'You just voted with your attention! You won’t see the traffic counter update for 24 hours because of Koi’s Gradual Consensus process.')]]")
        koii = driver.find_element(By.XPATH, "//*[text()[contains(.,'KOII earned')]]")
        koii_amount = re.search('.+?(?=KOII earned)', koii.text).group(0).strip()

        if true_view:
            db_service.write_statistic(nft_id=nft.id, proxy_id=proxy.id, views=views_amount, koii_rating=koii_amount)
        else:
            logger.info(f"Not feeling with proxy:{proxy.id} and nft:{nft.id} ")
    except Exception as ex:
        logger.error(f"Except with proxy:{proxy.id} and nft:{nft.id}")
        if 'msg' in ex.__dict__ and ex.__dict__['msg'] == "Not Found Element":
            db_service.set_failed_status_to_proxy(proxy)


def get_days_statistic(task):
    proxy, nft = task
    try:
        driver = driver_settings.driver_init(proxy_host=proxy.host, proxy_port=int(PROXY_PORT), proxy_user=PROXY_USER,
                                             proxy_password=PROXY_PASSWORD)
        driver.get(nft.url)

        time.sleep(random.randint(20, 30))

        views = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//*[text()[contains(.,'views')]]")), "Not Found Element"
        )
        views_amount = re.search('.+?(?=views)', views.text).group(0).strip()

        koii = driver.find_element(By.XPATH, "//*[text()[contains(.,'KOII earned')]]")
        koii_amount = re.search('.+?(?=KOII earned)', koii.text).group(0).strip()

        db_service.feeling_days_statistic(nft_id=nft.id, views=views_amount, koii_rating=koii_amount)
    except Exception as ex:
        logger.error(f"Except days statistic with proxy:{proxy.id} and nft:{nft.id}")
        if 'msg' in ex.__dict__ and ex.__dict__['msg'] == "Not Found Element":
            db_service.set_failed_status_to_proxy(proxy)


def main():
    logger.info(f"\nStart task ... \nDate:{datetime.now()}")
    nft_count = len(db_service.get_nft())
    proxies_count = len(db_service.get_proxies())
    count_proxy_to_one_nft = proxies_count // nft_count

    nfts = db_service.get_nft()
    for i in tqdm(range(len(nfts))):
        free_proxies = db_service.get_proxy_to_nft(nft_id=nfts[i].id, count_days=count_proxy_to_one_nft,
                                                   count_proxy_to_one_nft=count_proxy_to_one_nft)
        curr_nft = [nfts[i] for _ in range(count_proxy_to_one_nft)]
        tasks = [(free_proxies[j], curr_nft[j]) for j in range(len(free_proxies))]
        with Pool(processes=10) as pool:
            pool.map(nft_view_action, tasks)

    random_proxies = db_service.get_random_proxies(nft_count)
    tasks = [(random_proxies[j], nfts[j]) for j in range(nft_count)]
    with Pool(processes=10) as pool:
        pool.map(get_days_statistic, tasks)


if __name__ == '__main__':
    count_seconds_in_hour = 3600

    with open(os.path.join(BASE_DIR, 'start_time.txt'), 'r') as file:
        datetime_start = parser.parse(file.read())
        datetime_now = datetime.now()
        delta_hours = (datetime_now - datetime_start).total_seconds() // count_seconds_in_hour

    if delta_hours >= 30:
        with open(os.path.join(BASE_DIR, 'start_time.txt'), 'w') as file:
            file.write(f"{datetime.now()}")
        main()
