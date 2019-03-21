

from datetime import date
from google.cloud import bigquery, firestore

bg_client = bigquery.Client()

shema = [
    {"name": "date", "type": "DATE"},
    {"name": "id", "type": "STRING"},
    {"name": "bathrooms", "type": "INTEGER"},
    {"name": "bedrooms","type": "INTEGER"},
    {"name": "stories", "type": "INTEGER"},
    {"name": "type", "type": "STRING"},
    {"name": "sizeFrontage", "type": "STRING"},
    {"name": "size", "type": "INTEGER"},
    {"name": "postcode", "type": "STRING"},
    {"name": "address", "type": "STRING"},
    {"name": "longitude", "type": "FLOAT"},
    {"name": "latitude", "type": "FLOAT"},
    {"name": "garage", "type": "INTEGER"},
    {"name": "parking", "type": "INTEGER"},
    {"name": "price", "type": "INTEGER"}]

class RealtorBigQueryInjector():

    def __init__(self):
        self.bg_client = bigquery.Client()
        self.fs_client = firestore.Client()
        dataset_id = 'housing'
        table_id = 'realtor'
        self.table_ref =self.bg_client.dataset(dataset_id).table(table_id)
        self.table = self.bg_client.get_table(self.table_ref)
        self.houses = self.fs_client.collection('RealtorHouses')
        self.rows = []


    def save_bigquery(self, rows):
        errors = self.bg_client.insert_rows(self.table, rows)
        return errors

    def get_all_data(self, date_cutoff=date.today()):
        rows = []
        dates = list(map(lambda x: x, self.houses.get()))
        for date_i in dates:
            if date(*list(map(lambda x:int(x), date_i.id.split('-')))) >= date_cutoff:
                date_houses_ref = date_i._reference.collection('houses')
                date_houses = list(map(lambda x: x, date_houses_ref.get()))
                for house in date_houses:
                    house_data = house.to_dict()
                    bedrooms = self.get_safe(['Building', 'Bedrooms'], data=house_data, default=1)
                    if type(bedrooms) is str and '+' in bedrooms:
                        bedrooms = eval(bedrooms)
                    row = [date_i.id,
                           house_data['Id'],
                           int(float(house_data['Building']['BathroomTotal'])),
                           int(float(bedrooms)),
                           int(float((self.get_safe(['Building', 'StoriesTotal'], data=house_data, default=1)))),
                           self.get_safe(['Building','Type'], data=house_data) or 'N/A',
                           self.get_safe(['Land', 'SizeFrontage'], data=house_data) or 'N/A',
                           self.get_size(self.get_safe(['Land', 'SizeTotal'], data=house_data, default=0)),
                           house_data['PostalCode'],
                           house_data['Property']['Address']['AddressText'],
                           float(house_data['Property']['Address']['Longitude']),
                           float(house_data['Property']['Address']['Latitude']),
                           int(float(self.has_garage(self.get_safe(['Property', 'Parking'], data=house_data)))),
                           int(float(self.get_safe(['Property', 'ParkingSpaceTotal'], data=house_data, default=0))),
                           int(float(self.get_price(house_data['Property']['Price'].replace(',',''))))
                           ]
                    rows.append(row)

        return rows

    def get_safe(self, keys, data={}, default=None):
        try:
            d = data
            for key in keys:
                d = d[key]
            return d
        except KeyError:
            return default

    def get_price(self, price_str):
        price_str =  price_str.replace('$', '').replace(' ', '').replace('\xa0', '')
        if '+TPS+TVQ' in price_str:
            price_str = price_str.replace('+TPS+TVQ', '')
            price = int(price_str) * 1.15
        else:
            price = int(price_str)

        return price


    def has_garage(self, parking_list):
        try:
            for i in parking_list:
                if 'Garage' in i['Name']:
                    return 1
            return 0
        except:
            return 0

    def get_size(self, size_str):
        size=0
        if size_str == 0:
            return 0
        if 'm2' in size_str:
            size = float(size_str.replace('m2','')) * 10.764
        elif 'pi2' in size_str:
            size = float(size_str.replace('pi2', ''))
        elif 'X' in size_str:
            size = 0

        return int(size)

class DuproprioBigQueryInjector():

    def __init__(self):
        self.bg_client = bigquery.Client()
        self.fs_client = firestore.Client()
        dataset_id = 'housing'
        table_id = 'duproprio'
        self.table_ref =self.bg_client.dataset(dataset_id).table(table_id)
        self.table = self.bg_client.get_table(self.table_ref)
        self.houses = self.fs_client.collection('DuproprioHouses')
        self.rows = []

    def save_bigquery(self, rows):
        errors = self.bg_client.insert_rows(self.table, rows)
        return errors

    def get_all_data(self, date_cutoff=date.today()):
        rows = []
        dates = list(map(lambda x: x, self.houses.get()))
        for date_i in dates:
            if date(*list(map(lambda x: int(x), date_i.id.split('-')))) >= date_cutoff:
                date_houses_ref = date_i._reference.collection('houses')
                date_houses = list(map(lambda x: x, date_houses_ref.get()))
                for house in date_houses:
                    house_data = house.to_dict()

                    keys = list(house_data.keys())
                    try:
                        living_aera_index = [i for i, key, in enumerate(keys) if 'Livingspace' in key]
                        living_aera_key = keys[living_aera_index[0]]
                        living_space = int(house_data[living_aera_key].split('ft')[0])
                    except:
                        living_space = None

                    try:
                        lot = house_data['Lotdimensions']
                        lot = int(lot.split('ft')[0]) #bug if meter first in string
                    except:
                        lot = None

                    try:
                        year_constructed = int(house_data['Year of construction'])
                    except:
                        year_constructed = None

                    try:
                        levels = int(house_data['levels']) # can be level if only 1
                    except:
                        levels = None

                    try:
                        garage_data = house_data['Garage']
                        if len(garage_data) > 0:
                            garage = 1
                        elif 'Double' in garage_data:
                            garage = 2
                        elif 'Single' in garage_data:
                            garage = 1
                        elif 'Triple' in garage_data:
                            garage = 3
                        else:
                            garage = None
                    except:
                        garage = None

                    try:
                        municipal_eval = int(house_data['Municipal Assessment'].replace('$','').replace(',',''))
                    except:
                        municipal_eval = None

                    try:
                        bathrooms = int(house_data['bathrooms'])
                    except:
                        bathrooms = None

                    try:
                        bedrooms = int(house_data['bedrooms']) # can key to bathroom bedroom
                    except:
                        bedrooms = None

                    row = [
                        date_i.id,
                        house_data['address'],
                        levels,
                        living_space,
                        lot,
                        year_constructed,
                        garage,
                        house_data['ville'],
                        house_data['price'],
                        municipal_eval,
                        bathrooms,
                        bedrooms
                    ]

                    rows.append(row)
        return rows


# workerDuproprio = DuproprioBigQueryInjector()
# rows = workerDuproprio.get_all_data(date_cutoff=date(2019, 1, 1))
# workerDuproprio.save_bigquery(rows)

workerRealtor = RealtorBigQueryInjector()
rows = workerRealtor.get_all_data(date_cutoff=date(2019, 2, 16))
workerRealtor.save_bigquery(rows)