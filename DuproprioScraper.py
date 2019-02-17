
import asyncio
from datetime import datetime
from selenium import webdriver

from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By

from FirestoreWorker import FirestoreWorker
from Models import House

CHROMEDRIVER_PATH = '/Users/alexprovencher/Desktop/imopy/chromedriver'

cities = ['Rosemère', 'Sainte-Thèrese', 'Blainville', 'Terrebonne', 'Bois-des-filion',
          'boisbriand', 'loraine']

class DuproprioScrapper:
    pre_text = ''
    workers = []

    def __init__(self, cities):
        self.last_page = False
        self.options = Options()
        # self.options.headless = True
        self.options.add_argument('incognito')
        self.driver = webdriver.Chrome(CHROMEDRIVER_PATH, chrome_options=self.options)
        self.cities = cities
        self.duproprio_url = 'https://duproprio.com/en'
        self.duproprio_search_url = """https://duproprio.com/en/search/list?search=true&
            cities%5B0%5D=942&cities%5B1%5D=644&cities%5B2%5D=101&
            cities%5B3%5D=1019&cities%5B4%5D=108&cities%5B5%5D=106&
            cities%5B6%5D=1666&subtype%5B0%5D=1&subtype%5B1%5D=4&
            subtype%5B2%5D=5&subtype%5B3%5D=6&subtype%5B4%5D=7&
            subtype%5B5%5D=9&subtype%5B6%5D=10&subtype%5B7%5D=11&
            subtype%5B8%5D=13&subtype%5B9%5D=15&subtype%5B10%5D=17&
            subtype%5B11%5D=19&subtype%5B12%5D=21&subtype%5B13%5D=97&
            subtype%5B14%5D=98&subtype%5B15%5D=99&subtype%5B16%5D=100&
            subtype%5B17%5D=20&is_for_sale=1&with_builders=1&parent=1&
            pageNumber=1&sort=-published_at"""
        DuproprioScrapper.workers.append(self)
        self.driver.get(self.duproprio_search_url)
        self.search_results = []

    async def hit_search(self):
        try:
            self.driver.find_element_by_class_name('gtm-header-link-search-bar-button ').click()
        except:
            await asyncio.sleep(0.1)
            await self.hit_search()


    async def get_page_result(self):
        try:
            return self.driver.find_element_by_class_name('search-results-listings-list')
        except:
            await asyncio.sleep(0.1)
            return await self.get_page_result()


    async def process_page(self):
        self.page_result = await self.get_page_result()
        elems = self.page_result.find_elements(By.TAG_NAME, 'li')
        for e in elems:
            href = await asyncio.wait_for(self.get_href(e), 60)
            if href is not None:
                self.search_results.append(href)
            else:
                debug = e.get_attribute('innerHTML')
                print(debug)
            # self.search_results.append(
            #     e.find_element_by_class_name(
            #         'search-results-listings-list__item-bottom-container').find_element(
            #         By.TAG_NAME, 'a').get_attribute('href'))

    async def crawl(self):
        await self.process_page()
        next_btn = None
        try:
            next_btn = await asyncio.wait_for(self.get_next_btn(), 15)
        except:
            self.end_page = True
        try:
            await asyncio.wait_for(self.click_next(next_btn), 60)
        except Exception as e:
            print('crawl problems')
            print(e)

    async def click_next(self, btn):
        try:
            btn.click()
        except:
            await asyncio.sleep(0.1)
            return await self.click_next(btn)

    async def crawler(self):
        self.end_page = False
        while not self.end_page:
            await self.crawl()

    async def get_next_btn(self):
        try:
            return self.driver\
                .find_element_by_class_name('pagination__arrow--right')\
                .find_element_by_class_name('gtm-search-results-link-pagination-arrow ')
        except:
            await asyncio.sleep(0.1)
            return await self.get_next_btn()

    async def get_href(self, elem):
        try:
            return elem.find_element_by_class_name(
                'search-results-listings-list__item-bottom-container').find_element(
                By.TAG_NAME, 'a').get_attribute('href')
        except:
            try:
                if 'listing' not in elem.get_attribute('id'):
                    return
            except:
                await asyncio.sleep(0.1)
                return await self.get_href(elem)



