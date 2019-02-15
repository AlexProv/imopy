
import asyncio
from google.cloud import firestore

import CentrisScraper as CS


class FirestoreWorker():
    houses_crawled = []

    def __init__(self):
        self.db = firestore.Client()
        self.houses = self.db.collection('houses')
        self.houses_collection = self.houses.get()

        self.houses_collection = list(map(lambda x: x, self.houses_collection))
        self.ids = list(map(lambda x: x.id, self.houses_collection))

    async def inject(self):
        try:
            house = FirestoreWorker.houses_crawled.pop()
            # house = House(civicNumber=226, street='Des Genets', city='Rosemere')
            id = ''.join([str(house.civicNumber),'@', house.street, '@', house.city])
            if id not in self.ids:
                await self.add_to_firestore(id, house)
            else:
                pass
        except IndexError as e :
            print(e)

    async def work(self):
        done = False
        while not done:
            try:
                await self.inject()
                await asyncio.sleep(0.0001)
                situation = list(map(lambda x: x.last_page, CS.CentrisScrapper.workers))
                done = all(situation)
            except:
                pass


    async def add_to_firestore(self, id, house):
        self.houses.document(id).set(house.to_dict())



# self.houses.document('aaa').get().exists()

# doc_ref = db.collection('houses').where('asking_price', '<', 100)

# house_ref.document().set(House(asking_price=1, market_date=datetime.now()).to_dict())


# create a class with a static list of houses, create a coroutine to bulk add to firestore
# put the scraper in coroutine as well