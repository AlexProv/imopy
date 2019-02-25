import asyncio
from google.cloud import firestore
import DuproprioScraper as DS
import json
from  datetime import datetime

def runner(test):
    asyncio.run(test)

def gather_urls(dp, db):
    runner(dp.hit_search())
    runner(dp.crawler())
    db.collection('duproprio-urls').document(str(datetime.now())).set({'urls': json.dumps(dp.search_results)})

def gather_all_urls():
    dp_worker = DS.DuproprioScrapper(cities=DS.cities)
    gather_urls(dp_worker, firestore.Client())

def crawl_urls():
    db = firestore.Client()
    urls_gen = db.collection('duproprio-urls').get()
    urls = {}
    for url in urls_gen:
        urls[datetime.strptime(url.id, '%Y-%m-%d %H:%M:%S.%f')] = url.to_dict()

    latest = sorted(urls, reverse=True)[0]
    latest_urls = urls[latest]['urls']

    urls = list(map(lambda x: x.replace('"', '').replace(' ', ''), latest_urls[1:-2].split(',')))

    dp_worker = DS.DuproprioScrapper(start_url=urls.pop())
    dp_worker.page_crawler(urls)

crawl_urls()