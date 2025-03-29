# -*- coding: utf-8 -*-
import itertools
import logging
import re
import time
import traceback
from datetime import datetime

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver import ChromeOptions as Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

GM_WEBPAGE = 'https://www.google.com/maps/'
MAX_WAIT = 5
MAX_RETRY = 3
MAX_SCROLLS = 20

class GoogleMapsScraper:

    def __init__(self, debug=False):
        self.debug = debug
        self.driver = self.__get_driver()
        self.logger = self.__get_logger()
        self.wait = WebDriverWait(self.driver, MAX_WAIT)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        if exc_type is not None:
            traceback.print_exception(exc_type, exc_value, tb)

        self.driver.close()
        self.driver.quit()

        return True

    def sort_by(self, url, ind):
        self.driver.get(url)
        self.__click_on_cookie_agreement()

        try:
            menu_bt = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[data-value="Sort"]')))
            menu_bt.click()
            
            menu_items = self.wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div[role="menuitemradio"]')))
            if ind < len(menu_items):
                menu_items[ind].click()
                self.wait.until(EC.staleness_of(menu_bt))
                return 0
        except Exception as e:
            self.logger.warn(f'Failed to click sorting button: {str(e)}')
            return -1

    def get_places(self, keyword_list=None):
        df_places = pd.DataFrame()
        search_point_url_list = self._gen_search_points_from_square(keyword_list=keyword_list)

        for i, search_point_url in enumerate(search_point_url_list):
            print(f"Processing {i+1}/{len(search_point_url_list)}: {search_point_url}")

            if (i+1) % 10 == 0:
                print(f"Saving progress: {i}/{len(search_point_url_list)}")
                df_places = df_places[['search_point_url', 'href', 'name', 'rating', 'num_reviews', 'close_time', 'other']]
                df_places.to_csv('output/places_wax.csv', index=False)

            try:
                self.driver.get(search_point_url)
                results_container = self.wait.until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "div.m6QErb.DxyBCb.kA9KIf.dS8AEf.ecceSd > div[aria-label*='Results for']")
                ))
                
                last_height = self.driver.execute_script("return arguments[0].scrollHeight", results_container)
                scrolls = 0
                while scrolls < MAX_SCROLLS:
                    self.driver.execute_script('arguments[0].scrollTop = arguments[0].scrollHeight', results_container)
                    time.sleep(0.5)
                    new_height = self.driver.execute_script("return arguments[0].scrollHeight", results_container)
                    if new_height == last_height:
                        break
                    last_height = new_height
                    scrolls += 1

                response = BeautifulSoup(self.driver.page_source, 'html.parser')
                div_places = response.select('div[jsaction] > a[href]')

                for div_place in div_places:
                    place_info = {
                        'search_point_url': search_point_url.replace('https://www.google.com/maps/search/', ''),
                        'href': div_place['href'],
                        'name': div_place['aria-label']
                    }
                    df_places = df_places.append(place_info, ignore_index=True)

            except Exception as e:
                self.logger.error(f"Error processing {search_point_url}: {str(e)}")
                continue

        df_places = df_places[['search_point_url', 'href', 'name']]
        df_places.to_csv('output/places_wax.csv', index=False)

    def get_reviews(self, offset):
        # Wait for reviews container
        reviews_container = self.wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, 'div.m6QErb.DxyBCb.kA9KIf.dS8AEf')
        ))
        
        # Scroll with dynamic wait
        last_height = self.driver.execute_script("return arguments[0].scrollHeight", reviews_container)
        while True:
            self.driver.execute_script('arguments[0].scrollTop = arguments[0].scrollHeight', reviews_container)
            time.sleep(0.5)
            new_height = self.driver.execute_script("return arguments[0].scrollHeight", reviews_container)
            if new_height == last_height:
                break
            last_height = new_height

        buttons = self.driver.find_elements(By.CSS_SELECTOR, 'button.w8nwRe.kyuRq')
        for button in buttons:
            try:
                self.driver.execute_script("arguments[0].click();", button)
                time.sleep(0.2)
            except:
                continue

        response = BeautifulSoup(self.driver.page_source, 'html.parser')
        rblock = response.find_all('div', class_='jftiEf fontBodyMedium')
        parsed_reviews = []
        for index, review in enumerate(rblock):
            if index >= offset:
                r = self.__parse(review)
                parsed_reviews.append(r)
                print(r)

        return parsed_reviews

    def get_account(self, url):
        self.driver.get(url)
        self.__click_on_cookie_agreement()

        time.sleep(2)

        resp = BeautifulSoup(self.driver.page_source, 'html.parser')

        place_data = self.__parse_place(resp, url)

        return place_data

    def __parse(self, review):
        item = {}

        try:
            id_review = review['data-review-id']
        except Exception as e:
            id_review = None

        try:
            username = review['aria-label']
        except Exception as e:
            username = None

        try:
            review_text = self.__filter_string(review.find('span', class_='wiI7pd').text)
        except Exception as e:
            review_text = None

        try:
            rating = float(review.find('span', class_='kvMYJc')['aria-label'].split(' ')[0])
        except Exception as e:
            rating = None

        try:
            relative_date = review.find('span', class_='rsqaWe').text
        except Exception as e:
            relative_date = None

        try:
            n_reviews = review.find('div', class_='RfnDt').text.split(' ')[3]
        except Exception as e:
            n_reviews = 0

        try:
            user_url = review.find('button', class_='WEBjve')['data-href']
        except Exception as e:
            user_url = None

        item['id_review'] = id_review
        item['caption'] = review_text

        item['relative_date'] = relative_date

        item['retrieval_date'] = datetime.now()
        item['rating'] = rating
        item['username'] = username
        item['n_review_user'] = n_reviews
        item['url_user'] = user_url

        return item

    def __parse_place(self, response, url):
        place = {}

        try:
            place['name'] = response.find('h1', class_='DUwDvf fontHeadlineLarge').text.strip()
        except Exception as e:
            place['name'] = None

        try:
            place['overall_rating'] = float(response.find('div', class_='F7nice ').find('span', class_='ceNzKf')['aria-label'].split(' ')[1])
        except Exception as e:
            place['overall_rating'] = None

        try:
            place['n_reviews'] = int(response.find('div', class_='F7nice ').text.split('(')[1].replace(',', '').replace(')', ''))
        except Exception as e:
            place['n_reviews'] = 0

        try:
            place['n_photos'] = int(response.find('div', class_='YkuOqf').text.replace('.', '').replace(',','').split(' ')[0])
        except Exception as e:
            place['n_photos'] = 0

        try:
            place['category'] = response.find('button', jsaction='pane.rating.category').text.strip()
        except Exception as e:
            place['category'] = None

        try:
            place['description'] = response.find('div', class_='PYvSYb').text.strip()
        except Exception as e:
            place['description'] = None

        b_list = response.find_all('div', class_='Io6YTe fontBodyMedium')
        try:
            place['address'] = b_list[0].text
        except Exception as e:
            place['address'] = None

        try:
            place['website'] = b_list[1].text
        except Exception as e:
            place['website'] = None

        try:
            place['phone_number'] = b_list[2].text
        except Exception as e:
            place['phone_number'] = None
    
        try:
            place['plus_code'] = b_list[3].text
        except Exception as e:
            place['plus_code'] = None

        try:
            place['opening_hours'] = response.find('div', class_='t39EBf GUrTXd')['aria-label'].replace('\u202f', ' ')
        except:
            place['opening_hours'] = None

        place['url'] = url

        lat, long, z = url.split('/')[6].split(',')
        place['lat'] = lat[1:]
        place['long'] = long

        return place

    def _gen_search_points_from_square(self, keyword_list=None):
        keyword_list = [] if keyword_list is None else keyword_list

        square_points = pd.read_csv('input/square_points.csv')

        cities = square_points['city'].unique()

        search_urls = []

        for city in cities:

            df_aux = square_points[square_points['city'] == city]
            latitudes = df_aux['latitude'].unique()
            longitudes = df_aux['longitude'].unique()
            coordinates_list = list(itertools.product(latitudes, longitudes, keyword_list))

            search_urls += [f"https://www.google.com/maps/search/{coordinates[2]}/@{str(coordinates[1])},{str(coordinates[0])},{str(15)}z"
             for coordinates in coordinates_list]

        return search_urls

    def __expand_reviews(self):
        buttons = self.driver.find_elements(By.CSS_SELECTOR,'button.w8nwRe.kyuRq')
        for button in buttons:
            self.driver.execute_script("arguments[0].click();", button)

    def __scroll(self):
        scrollable_div = self.driver.find_element(By.CSS_SELECTOR,'div.m6QErb.DxyBCb.kA9KIf.dS8AEf')
        self.driver.execute_script('arguments[0].scrollTop = arguments[0].scrollHeight', scrollable_div)

    def __get_logger(self):
        logger = logging.getLogger('googlemaps-scraper')
        logger.setLevel(logging.DEBUG)

        fh = logging.FileHandler('gm-scraper.log')
        fh.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        fh.setFormatter(formatter)

        logger.addHandler(fh)

        return logger

    def __get_driver(self):
        options = Options()
        if not self.debug:
            options.add_argument('--headless')
        else:
            options.add_argument('--window-size=1366,768')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-notifications')
        options.add_argument('--accept-lang=en-GB')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-logging')
        options.add_argument('--disable-infobars')
        options.add_argument('--disable-default-apps')
        options.add_argument('--disable-popup-blocking')
        options.add_argument('--disable-save-password-bubble')
        options.add_argument('--disable-translate')
        options.add_argument('--disable-web-security')
        options.add_argument('--disable-features=IsolateOrigins,site-per-process')
        options.add_argument('--disable-site-isolation-trials')
        options.add_argument('--disable-features=NetworkService')
        options.add_argument('--disable-features=VizDisplayCompositor')
        options.add_argument('--disable-features=NetworkServiceInProcess')
        
        service = Service()
        input_driver = webdriver.Chrome(service=service, options=options)
        
        input_driver.get(GM_WEBPAGE)
        
        return input_driver

    def __click_on_cookie_agreement(self):
        try:
            agree = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, '//span[contains(text(), "Reject all")]')))
            agree.click()

            return True
        except:
            return False

    def __filter_string(self, str):
        strOut = str.replace('\r', ' ').replace('\n', ' ').replace('\t', ' ')
        return strOut
