import asyncio
from google.cloud import firestore
import DuproprioScraper as DS
import json
from  datetime import datetime

def runner(test):
    asyncio.run(test)


loop = asyncio.get_event_loop()

dp_worker = DS.DuproprioScrapper(DS.cities)
runner(dp_worker.hit_search())
runner(dp_worker.crawler())


db = firestore.Client()
db.collection('duproprio-urls').document('urls').set({str(datetime.now()):json.dumps(dp_worker.search_results)})
