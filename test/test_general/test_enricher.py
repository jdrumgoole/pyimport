import pprint
import time

from pyimport.enricher import Enricher
from pyimport.fieldfile import FieldFile

data = [{'RateCodeID': '1', 'VendorID': '2', 'dropoff_latitude': '40.743377685546875',
         'dropoff_longitude': '-73.973762512207031', 'extra': '0.5', 'fare_amount': '7.5', 'mta_tax': '0.5',
         'passenger_count': '1', 'payment_type': '2', 'pickup_latitude': '40.724250793457031',
         'pickup_longitude': '-73.987686157226562', 'store_and_fwd_flag': 'N', 'tip_amount': '0', 'tolls_amount': '0',
         'total_amount': '8.8', 'tpep_dropoff_datetime': '2015-01-08 22:50:56',
         'tpep_pickup_datetime': '2015-01-08 22:44:09', 'trip_distance': '1.55'},
        {'RateCodeID': '1', 'VendorID': '1', 'dropoff_latitude': '40.721080780029297',
         'dropoff_longitude': '-74.004104614257813', 'extra': '0.5', 'fare_amount': '7', 'mta_tax': '0.5',
         'passenger_count': '3', 'payment_type': '2', 'pickup_latitude': '40.726932525634766',
         'pickup_longitude': '-73.991569519042969', 'store_and_fwd_flag': 'N', 'tip_amount': '0', 'tolls_amount': '0',
         'total_amount': '8.3', 'tpep_dropoff_datetime': '2015-01-08 22:51:17',
         'tpep_pickup_datetime': '2015-01-08 22:44:09', 'trip_distance': '1.20'},
        {'RateCodeID': '1', 'VendorID': '1', 'dropoff_latitude': '40.798198699951172',
         'dropoff_longitude': '-73.952354431152344', 'extra': '0.5', 'fare_amount': '10.5', 'mta_tax': '0.5',
         'passenger_count': '1', 'payment_type': '2', 'pickup_latitude': '40.783443450927734',
         'pickup_longitude': '-73.981918334960938', 'store_and_fwd_flag': 'N', 'tip_amount': '0', 'tolls_amount': '0',
         'total_amount': '11.8', 'tpep_dropoff_datetime': '2015-01-08 22:55:27',
         'tpep_pickup_datetime': '2015-01-08 22:44:10', 'trip_distance': '2.40'},
        {'RateCodeID': '1', 'VendorID': '1', 'dropoff_latitude': '40.832000732421875',
         'dropoff_longitude': '-73.919570922851563', 'extra': '0.5', 'fare_amount': '21.5', 'mta_tax': '0.5',
         'passenger_count': '1', 'payment_type': '2', 'pickup_latitude': '40.743553161621094',
         'pickup_longitude': '-73.973121643066406', 'store_and_fwd_flag': 'N', 'tip_amount': '0', 'tolls_amount': '0',
         'total_amount': '22.8', 'tpep_dropoff_datetime': '2015-01-08 22:58:09',
         'tpep_pickup_datetime': '2015-01-08 22:44:10', 'trip_distance': '7.30'},
        {'RateCodeID': '1', 'VendorID': '1', 'dropoff_latitude': '40.764053344726562',
         'dropoff_longitude': '-73.984390258789063', 'extra': '0.5', 'fare_amount': '3.5', 'mta_tax': '0.5',
         'passenger_count': '1', 'payment_type': '2', 'pickup_latitude': '40.766208648681641',
         'pickup_longitude': '-73.982948303222656', 'store_and_fwd_flag': 'N', 'tip_amount': '0', 'tolls_amount': '0',
         'total_amount': '4.8', 'tpep_dropoff_datetime': '2015-01-08 22:46:16',
         'tpep_pickup_datetime': '2015-01-08 22:44:12', 'trip_distance': '.40'},

        {'RateCodeID': '1', 'VendorID': '2', 'dropoff_latitude': '40.757778167724609',
         'dropoff_longitude': '-73.974647521972656', 'extra': '0.5', 'fare_amount': '5', 'mta_tax': '0.5',
         'passenger_count': '1', 'payment_type': '2', 'pickup_latitude': '40.764019012451172',
         'pickup_longitude': '-73.982498168945313', 'store_and_fwd_flag': 'N', 'tip_amount': '0', 'tolls_amount': '0',
         'total_amount': '6.3', 'tpep_dropoff_datetime': '2015-01-08 22:48:33',
         'tpep_pickup_datetime': '2015-01-08 22:44:12', 'trip_distance': '.71'},
        {'RateCodeID': '1', 'VendorID': '1', 'dropoff_latitude': '40.740146636962891',
         'dropoff_longitude': '-73.986076354980469', 'extra': '0.5', 'fare_amount': '7.5', 'mta_tax': '0.5',
         'passenger_count': '1', 'payment_type': '1', 'pickup_latitude': '40.759346008300781',
         'pickup_longitude': '-73.972160339355469', 'store_and_fwd_flag': 'N', 'tip_amount': '1.2', 'tolls_amount': '0',
         'total_amount': '10', 'tpep_dropoff_datetime': '2015-01-08 22:51:35',
         'tpep_pickup_datetime': '2015-01-08 22:44:12', 'trip_distance': '1.60'},
        {'RateCodeID': '1', 'VendorID': '2', 'dropoff_latitude': '40.797061920166016',
         'dropoff_longitude': '-73.971282958984375', 'extra': '0.5', 'fare_amount': '4', 'mta_tax': '0.5',
         'passenger_count': '1', 'payment_type': '1', 'pickup_latitude': '40.793289184570313',
         'pickup_longitude': '-73.972679138183594', 'store_and_fwd_flag': 'N', 'tip_amount': '0.9', 'tolls_amount': '0',
         'total_amount': '6.2', 'tpep_dropoff_datetime': '2015-01-08 22:46:37',
         'tpep_pickup_datetime': '2015-01-08 22:44:13', 'trip_distance': '.52'},
        {'RateCodeID': '1', 'VendorID': '2', 'dropoff_latitude': '40.762882232666016',
         'dropoff_longitude': '-73.95947265625', 'extra': '0.5', 'fare_amount': '18.5', 'mta_tax': '0.5',
         'passenger_count': '3', 'payment_type': '1', 'pickup_latitude': '40.702728271484375',
         'pickup_longitude': '-74.011489868164063', 'store_and_fwd_flag': 'N', 'tip_amount': '3.8', 'tolls_amount': '0',
         'total_amount': '23.6', 'tpep_dropoff_datetime': '2015-01-08 22:55:05',
         'tpep_pickup_datetime': '2015-01-08 22:44:14', 'trip_distance': '6.23'}]


def run_enrich_it(iters):
    ff = FieldFile.load("../test_command/yellow_trip.tff")
    enricher = Enricher(field_file=ff)
    start = time.time()
    for _ in range(iters):
        for doc in data:
            new_doc = enricher.enrich_doc(doc)
            assert new_doc is not None
    end = time.time()
    return end - start


def test_run_enrich_it():
    total_elapsed = 0

    for i in range(100):
        elapsed = run_enrich_it(100)
        total_elapsed = total_elapsed + elapsed

    print(f"average run time : {total_elapsed / 10}")
