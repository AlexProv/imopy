

import requests
import calendar
import argparse
from datetime import date, timedelta

from urllib.parse import urlencode
from google.cloud import firestore

from Models import injectArguments

class RealtorWorker:

    realtor_url = 'https://api2.realtor.ca/Listing.svc/PropertySearch_Post'

    def __init__(self):
        self.db = firestore.Client()
        self.session = requests.session()
        self.houses = self.db.collection('RealtorHouses')
        self.results_ids = set()
        self.results = {}
        self.payload = Payload.create_payload()

    def daily_houses(self, target_date):
        if target_date < date.today() + timedelta(days=1):
            delta = date.today() - target_date
            results = self._get_houses(delta.days)
            self.results[str(target_date)] = results
        else:
            raise Exception('Future date {}'.format(target_date))

    def monthly_houses(self, month, year=date.today().year, inner_call=False):
        if not inner_call:
            day_afther = date.today() + timedelta(days=-1)
            self.daily_houses(day_afther)
            self.results = {}

        nb_days = calendar.monthrange(year, month)[1]

        days = [date(year, month, day) for day in range(1, nb_days + 1)]

        sorted_days = sorted(days, key=lambda x: x)
        sorted_days.reverse()
        for day in sorted_days:
            if day < date.today():
                self.daily_houses(day)

    def yearly_houses(self, year):
        day_afther = date.today() + timedelta(days=-1)
        self.daily_houses(day_afther)
        self.results = {}

        months = [i for i in range(1,13)]
        months.reverse()
        for m in months:
            self.monthly_houses(m, year, inner_call=True)

    def _get_houses(self, day):
        self.payload.NumberOfDays = day
        req = self.session.post(RealtorWorker.realtor_url, data=self.payload.urlencoded())
        data = req.json()
        paging = data['Paging']

        results = []
        results += data['Results']
        while self.payload.CurrentPage < paging['TotalPages']:
            results += self.get_next_page_results()

        return self._add_results(results)
        # results = self._results_set(results)

    def _add_results(self, results):
        results_ids_set = self._results_ids_set(results)
        diff_ids = (results_ids_set ^ self.results_ids) & results_ids_set
        results_dict = self._results_dict(results)
        currated_results = {}
        for id in diff_ids:
            currated_results[id] = results_dict[id]

        self.results_ids.update(results_ids_set)
        return currated_results


    def _results_ids_set(self, results):
        r = []
        for i in results:
            r.append(i['Id'])
        return set(r)

    def _results_dict(self, results):
        r = {}
        for i in results:
            r[i['Id']] = i
        return r

    def get_next_page_results(self):
        self.payload.CurrentPage += 1
        req = self.session.post(RealtorWorker.realtor_url, data=self.payload.urlencoded())
        data = req.json()
        return data['Results']

    def save_db(self):
        for date_id, result in self.results.items():
            date_ref = self.houses.document(date_id)
            if not date_ref.get().exists and result != {}:
                date_ref.set({'exists':True})
            else:
                print(date_id)
            for id, result in result.items():
                # date_ref.set()
                house_ref = date_ref.collection('houses').document(id)
                house = house_ref.get()
                if house.exists:
                    house_ref.update(result)
                else:
                    house_ref.set(result)




class Payload():
    search_payload = {
        'ZoomLevel': 11,
        'LatitudeMax': 45.6804319,
        'LongitudeMax': -73.7042413,
        'LatitudeMin': 45.5974969,
        'LongitudeMin': -73.8657747,
        'CurrentPage': 1,
        'PropertyTypeGroupID': 1,
        'PropertySearchTypeId': 1,
        'TransactionTypeId': 2,
        'PriceMin': 0,
        'PriceMax': 0,
        'NumberOfDays': 2,
        'BedRange': "0 - 0",
        'BathRange': "0 - 0",
        'RecordsPerPage': 12,
        'ApplicationId': 1,
        'CultureId': 2,
        'Version': 7.0
    }

    @staticmethod
    def create_payload():
        return Payload(**Payload.search_payload)

    @injectArguments
    def __init__(self, **kwargs):
        pass

    def to_dict(self):
        return self.__dict__

    def urlencoded(self):
        return urlencode(self.__dict__)


rw = RealtorWorker()
parser = argparse.ArgumentParser()
parser.add_argument('--run', help='mode: current_month')
args = parser.parse_args()
if args.run == 'current_month':
    rw.monthly_houses(date.today().month)
    rw.save_db()