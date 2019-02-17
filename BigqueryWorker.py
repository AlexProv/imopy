
from google.cloud import bigquery, firestore

bg_client = bigquery.Client()

query = 'select * from `imopy-analytics.housing.houses`'

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

class RealtorBigQueryWorker():

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

    def get_all_data(self):
        rows = []
        dates = list(map(lambda x: x, self.houses.get()))
        for date in dates:
            date_houses_ref = date._reference.collection('houses')
            date_houses = list(map(lambda x: x, date_houses_ref.get()))
            for house in date_houses:
                house_data = house.to_dict()
                row = [date.id,
                       house_data['Id'],
                       int(float(house_data['Building']['BathroomTotal'])),
                       int(float(self.get_safe(['Building', 'Bedrooms'], data=house_data, default=1))),
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
                       int(float(self.get_price(house_data['Property']['Price'])))
                       ]
                rows.append(row)

        self.rows = rows
        print(rows)

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
            # a,b = size_str.split('X')
            # a = float(a[0])
            # b = float(b[0].replace('irr', ''))
            #
            # size = a * b * 10.764
            size = 0

        return int(size)


worker = RealtorBigQueryWorker()
worker.get_all_data()
worker.save_bigquery(worker.rows)
# client = bigquery.Client()
# dataset_id = 'my_dataset'  # replace with your dataset ID
# For this sample, the table must already exist and have a defined schema
# table_id = 'my_table'  # replace with your table ID
# table_ref = client.dataset(dataset_id).table(table_id)
# table = client.get_table(table_ref)  # API request
#
# rows_to_insert = [
#     (u'Phred Phlyntstone', 32),
#     (u'Wylma Phlyntstone', 29),
# ]
#
# errors = client.insert_rows(table, rows_to_insert)  # API request
#
# assert errors == []