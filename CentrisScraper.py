
import asyncio
from datetime import datetime
from selenium import webdriver

from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By

import FirestoreWorker as FW
from Models import House

CHROMEDRIVER_PATH = '/Users/alexprovencher/Desktop/imopy/chromedriver'

cities = ['Rosemère', 'Sainte-Thèrese', 'Blainville', 'Terrebonne', 'Bois-des-filion',
          'boisbriand', 'loraine']

class CentrisScrapper:

    workers = []

    def __init__(self, cities):
        self.last_page = False
        self.pre_text = ''
        self.options = Options()
        # self.options.headless = True
        self.options.add_argument('incognito')
        self.driver = webdriver.Chrome(CHROMEDRIVER_PATH, chrome_options=self.options)
        self.cities = cities
        self.centris_url = 'https://www.centris.ca/en'
        CentrisScrapper.workers.append(self)


    async def wait_load(self, elem):
        if elem.is_displayed():
            self.pre_text = elem.get_attribute('innerHTML')
            return True
        else:
            await asyncio.sleep(0.01)
            await self.wait_load(elem)


    async def add_search_params(self):
        search = self.driver.find_element_by_id('search')
        for city in cities: 
            search.send_keys(city)
            search_box = self.driver.find_element_by_id('ui-id-2')
            await CentrisScrapper.wait_load(search_box)
            search.send_keys(Keys.ENTER)
            await asyncio.sleep(0.05)
        show = self.driver.find_element_by_id('btn-advanced-criterias')
        show.click()

        self.driver.find_element_by_id('IS').click()
        property_type = self.driver.find_element_by_id('item-property')
        maison_uni = property_type.find_element_by_class_name('btn-form-choice')
        maison_uni.click()
        self.current_url = self.driver.current_url

    async def get_first_house(self):
        try:
            if self.current_url == self.driver.current_url:
                raise Exception('still on same page')
            self.driver.find_element_by_class_name('a-more-detail').click()
        except Exception as e:
            await asyncio.sleep(0.01)
            await asyncio.wait_for(self.get_first_house(), 60)

    async def search_setup(self):
        self.driver.get(self.centris_url)        
        await self.add_search_params()
        await asyncio.sleep(0.05)
        search_btn = self.driver.find_element_by_id('submit-search')
        search_btn.click()
        await self.get_first_house()

    def crawler(self):
        self.current_url = self.driver.current_url
        address_elem = self.driver.find_element_by_class_name('address')
        address_data = address_elem.text.split(',')
        if len(address_data) >= 4:
            civic_number, street, city, neighbourhood = address_data
        else:
            civic_number, street, city = address_data
            neighbourhood = None
            # can be reached before page load

        buy_price = self.driver.find_element_by_id('BuyPrice').get_attribute('content')

        geo_streetview = self.driver.find_element_by_class_name('streetview')
        latlng = geo_streetview.find_element(By.TAG_NAME, 'a').get_attribute('onclick')
        try:
            lat, lng = latlng.split('=')[1].split('&')[0].split(',')
        except:
            lat, lng = None, None

        teaser = self.driver.find_element_by_class_name('teaser')
        rooms, bedrooms, bathrooms = teaser.find_elements(By.TAG_NAME, 'span')
        rooms = int(rooms.text.split(' ')[0])
        spliter = lambda x : (int(x[0]), int(x[1]))
        bedrooms = bedrooms.text.split(' ')[0]

        bedrooms_above, bedrooms_basement = spliter(bedrooms.split('+'))
        bathrooms = bathrooms.text.split(' ')[0]
        bathrooms_full, toilet = spliter(bathrooms.split('+'))

        features__elem_a, features__elem_b = self.driver.find_elements(By.TAG_NAME, 'table')[0:2]
        features = self.get_features(features__elem_a.text)
        features.update(self.get_features(features__elem_b.text))
        features = self.format_features(features)

        FW.FirestoreWorker.houses_crawled.append(House(
            marketDate=datetime.today(), sellingPrice=buy_price, salesUrl=self.driver.current_url,
            city=city, neighbourhood=neighbourhood, street=street, civicNumber=civic_number,
            lat=lat, lng=lng, rooms=rooms, bedrooms=bedrooms, bedroomsAbove=bedrooms_above,
            bedroomsBasement=bedrooms_basement, bathrooms=bathrooms_full, toilet=toilet,
            yearBuilt=features['Year'], features=features))


    def get_features(self, data):
        features_data = data.split('\n')
        features = {}
        for f in features_data:
            k, *v = f.split(' ')
            features[k] = ''.join(map(str, v))

        return features

    def format_features(self, features):
        for k,v in features.items():
            try:
                if k == 'Building':
                    features[k] = v.split('style')[0]
                elif k == 'Year':
                    features[k] = int(v.split('built')[1])
                elif k == 'Lot':
                    v = v.split('area')[1]
                    features[k] = {'size': int(v[:-4].replace(',','')),
                                   'unit': v[-4:]}
                elif k == 'Parking':
                    temp = v.split(',')
                    for i in temp:
                        i = i.split('(')
                        features[k] = {i[0]: int(i[1].replace(')',''))}
                elif k == 'Pool':
                    features[k] = v
                elif 'Fireplace' in k:
                    features[k] = v
            except Exception as e:
                print(e)
        return features

    async def get_next_house(self):
        try:
            self.driver.find_element_by_class_name('next').click()
            await self.wait_next_load()
        except Exception as e:
            self.last_page = True

    async def wait_next_load(self):
        try:
            if self.current_url == self.driver.current_url:
                raise Exception('still on same page')
            self.driver.find_element_by_id('thumbnailPhotoUrl')
            return True
        except:
            await asyncio.wait_for(self.wait_next_load(), 60)

    async def crawl(self):
        await self.search_setup()
        while(not self.last_page):
            self.crawler()
            await self.get_next_house()