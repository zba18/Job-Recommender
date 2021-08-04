from dotenv import load_dotenv
import os

load_dotenv()
secret = os.getenv('SECRET_KEY')
print(f"secret:{secret}")

import requests
import time 
import hashlib

def good_hash_header():
    tt = str(time.time_ns())
    hash_ = hashlib.new('sha512')
    hash_.update(f"{tt}-{secret}".encode())
    header = {'time-token': tt, 'auth-key': hash_.hexdigest()}
    return header

import redis
import h3
import numpy as np

r = redis.Redis('localhost')
for key in r.scan_iter("prefix:*"):
    r.delete(key)

url = 'http://127.0.0.1:8002/add_job'

for i in range(100):
    if i < 50:
        body = {'id_': i,
          "lat": 0,
          "lng": 0
        }
    else:
        body = {'id_': i,
          "lat": 4,
          "lng": 4
        }
    #print(body)
    x = requests.post(url, json = body, headers = good_hash_header())

# some data already exists.
url = 'http://127.0.0.1:8002/submit_analytics'
body = {'feeds':  ## rename this?
        {
            'all': {'impressions':list(range(40)), 'applies':[], 'views':[], 'saves':[]},
            'click/impression':{'impressions':[0,1,2], 'applies':[0], 'views':[0], 'saves':[0]},
            'applies/impression':{'impressions':list(range(10)), 'applies':[], 'views':[], 'saves':[3]},
        }
       }
print(body)

#use the 'headers' parameter to set the HTTP headers:
x = requests.post(url, json = body, headers = good_hash_header())
print(x.text)
