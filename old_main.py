import h3
import redis
import numpy as np

r = redis.Redis('localhost') # make sure this is running, port default is 6379

def add_job(id_, coords):
    hash_ = h3.geo_to_h3(*coords, 7)
    print(hash_)
    return r.append(hash_, f'{id_} ')# add a space after to separate atomically

def find_jobs(coords, radius = 5):
    ## RADIUS CALC for later
    hash_ = h3.geo_to_h3(*coords, 7)
    str_list = r.mget( h3.k_ring(hash_, 3) ) #redis always returns bytes 
    ids = b''.join( (x for x in str_list if x ) ).split(b' ') # join if not None, then split by space
    ids = [int(x) for x in ids if x!= b'']
    return ids

from fastapi import FastAPI

app = FastAPI()

@app.post("/add_job")
async def add_job_service(id: int, lat: float, lng: float):
    resp = add_job(id, (lat,lng))
    return {"message": resp}

@app.post("/find_jobs")
async def find_job_service(lat: float, lng: float):
    resp = find_jobs((lat,lng))
    return {"job_ids": resp}