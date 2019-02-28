import os, sys
from datetime import date
from selenium import webdriver

from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from google.cloud import firestore

# CHROMEDRIVER_PATH = '/Users/alexprovencher/Desktop/imopy/chromedriver'

try:
   CHROMEDRIVER_PATH = os.environ["WEBDRIVER_PATH"]
except KeyError:
   sys.exit(1)

cities = ['Rosemère', 'Sainte-Thèrese', 'Blainville', 'Terrebonne', 'Bois-des-filion',
          'boisbriand', 'loraine']

class DuproprioScrapper:
    pre_text = ''
    workers = []

    def __init__(self, cities=None, start_url=None):
        self.last_page = False
        self.options = Options()
        self.options.headless = True

        self.options.add_argument('incognito')
        self.driver = webdriver.Chrome(CHROMEDRIVER_PATH, chrome_options=self.options)
        self.driver.set_window_size(1080, 1920)
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
        if start_url:
            self.driver.get(start_url)
        else:
            self.driver.get(self.duproprio_search_url)
        self.search_results = []
        self.houses = {}

        page_state = self.driver.execute_script('return document.readyState;')

    def hit_search(self):
        page_state = self.driver.execute_script('return document.readyState;')
        self.driver.get_screenshot_as_file('/Users/alexprovencher/Desktop/Tester.png')
        try:
            self.driver.find_element_by_class_name('gtm-header-link-search-bar-button ').click()
        except Exception as e:
            print(e)
            try:
                popup = self.driver.find_element_by_class_name('info-sessions-popup__close-icon')
                popup.click()
            except Exception as e:
                print(e)
            self.hit_search()


    def get_page_result(self):
        try:
            return self.driver.find_element_by_class_name('search-results-listings-list')
        except:
            return self.get_page_result()


    def process_page(self):
        self.page_result = self.get_page_result()
        elems = self.page_result.find_elements(By.TAG_NAME, 'li')
        for e in elems:
            href = self.get_href(e)
            if href is not None:
                self.search_results.append(href)
            else:
                debug = e.get_attribute('innerHTML')
                print(debug)

    def crawl(self):
        self.process_page()
        next_btn = None
        try:
            next_btn = self.get_next_btn()
        except:
            self.end_page = True
        try:
            try:
                popup = self.driver.find_element_by_class_name('info-sessions-popup__close-icon')
                popup.click()
            except:
                pass
            self.click_next(next_btn)

        except Exception as e:
            print('crawl problems')
            print(e)

    def click_next(self, btn):
        btn.click()
        page_state = self.driver.execute_script('return document.readyState;')

    def crawler(self):
        self.end_page = False
        while not self.end_page:
            print('crawling {}'.format(self.driver.current_url))
            self.crawl()
        self.driver.close()

    def get_next_btn(self):
        return self.driver\
                .find_element_by_class_name('pagination__arrow--right')\
                .find_element_by_class_name('gtm-search-results-link-pagination-arrow ')

    def get_href(self, elem):
        try:
            return elem.find_element_by_class_name(
                'search-results-listings-list__item-bottom-container').find_element(
                By.TAG_NAME, 'a').get_attribute('href')
        except:
            try:
                if 'listing' not in elem.get_attribute('id'):
                    return
            except:
                return self.get_href(elem)

    def page_crawl(self):
        page_state = self.driver.execute_script('return document.readyState;')

        price = self.driver.find_element_by_xpath("//meta[@property='price']").get_property('content')
        price_currency = self.driver.find_element_by_xpath("//meta[@property='priceCurrency']").get_property('content')

        address = self.driver.find_element_by_class_name('listing-location__address')
        rue = address.find_element_by_css_selector('h1').get_attribute('innerHTML')
        ville = address.find_element_by_css_selector('h2').find_element_by_css_selector('a').get_attribute('innerHTML')
        try:
            region = ville.split(' ')[0]
            if ' ' in ville:
                ville = ville.split(' ')[1].replace('(','').replace(')','')
        except:
            pass

        features = {}
        main_items = self.driver.find_elements_by_class_name('listing-main-characteristics__item')
        for item in main_items:
            item_number = item.find_element_by_class_name('listing-main-characteristics__number').get_attribute('innerHTML')
            item_number = item_number.replace(' ', '').replace('\n', '')
            item_key = item.find_element_by_class_name('listing-main-characteristics__title').get_attribute('innerHTML')
            item_key = item_key.replace(' ', '').replace('\n', '')
            features[item_key] = item_number

        property_features = self.driver.find_elements_by_class_name('listing-list-characteristics__table')
        for item in property_features:
            item_key = item.find_element_by_class_name('listing-list-characteristics__row--label').get_attribute(
                'innerHTML')
            item_value = item.find_element_by_class_name('listing-list-characteristics__row--value').get_attribute(
                'innerHTML')
            features[item_key] = item_value

        extended_features = self.driver.find_elements_by_class_name('listing-complete-list-characteristics__content__group')
        for item in extended_features:
            item_key = item.find_element_by_class_name('listing-complete-list-characteristics__content__group__title').get_attribute(
                'innerHTML')
            values = []
            values_html = item.find_elements_by_class_name('listing-complete-list-characteristics__content__group__item')
            for v in values_html:
                values.append(v.get_attribute('innerHTML'))
            features[item_key] = values

        features['ville'] = ville
        features['region'] = region
        features['address'] = rue
        features['price'] = price
        features['price-currency'] = price_currency
        features['url'] = self.driver.current_url

        self.houses['{} {}'.format(rue, ville)] = features
        return ('{} {}'.format(rue, ville), features)

    def page_crawler(self, urls):
        db = firestore.Client()
        for url in urls:
            id, features = self.page_crawl()

            try:
                # save to database, will need to reformat for multiple workers later
                date_ref = db.collection('DuproprioHouses').document(str(date.today()))
                if not date_ref.get().exists:
                    date_ref.set({'exists': True})
                house_ref = date_ref.collection('houses').document(id)
                house = house_ref.get()
                if house.exists:
                    house_ref.update(features)
                else:
                    house_ref.set(features)
            except:
                print('problem with {}'.format(self.driver.current_url))

            self.driver.get(url)

        self.driver.close()

