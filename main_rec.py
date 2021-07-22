import redis
from fastapi import FastAPI, HTTPException, Depends, Request, Body
from typing import List

import logging

from models import FindJobRequest, AddJobRequest, EditJobRequest, DelJobRequest, AnalyticsRequest
from h3_redis import H3RedisWorker
from bandit_master import BanditMaster, StatsKeeper
from simple_security import verify_user

from fastapi_utils.tasks import repeat_every


app = FastAPI()

local_db = 'analytics.db' 

main_api_db = None
#connect to main_api_db here


redis_con = redis.Redis('localhost') # make sure this is running, port default is 6379

h3worker = H3RedisWorker(redis_con)
stats_keeper = StatsKeeper(local_db) # add main_api_db later. by default, uses 'fake_main_behaviors.db' file locally.
bandit_master = BanditMaster(stats_keeper)

'''
@app.on_event("startup")
def load_resources_for_startup():
    init_db_connections()
    
    # bandit master
    load_bandit_master_from_db()
    generate_feed_decisions()
    
    #stats keeper
    load_ad_stats_from_db()
'''

@app.on_event("startup")
@repeat_every(seconds= 5, wait_first=True)  # 5 minutes
def run_background_tasks():
    bandit_master.process_analytics()
    

    
### ENDPOINTS 

# rec feed

@app.get("/request_rec_feed")
async def rec_feed(lat:float, lng:float, Depends = None):
    nearby_job_list = h3worker.find_jobs((lat,lng))
    nearby_jobs_df = stats_keeper.ad_stats_df.loc[nearby_job_list]

    chosen_feed_num = next(bandit_master.feed_decision_generator)

    #need to have a list here that maps column names (Clickthrough, apply/impression, etc)   
    feed_col = bandit_master.keys2feed_names[chosen_feed_num] 
    
    ordered_job_ids = nearby_jobs_df.sort_values(by = feed_col).index #this sorts the jobs by the feed column `.index` returns job_id
    ##TO CHECK LATER: THAT INDEX OF AD_STATS matches job_id
    
    return {'rec_feed': ordered_job_ids} # on main api, need to use this to preserve the feed order. https://stackoverflow.com/a/36664472

@app.post("/submit_analytics")
async def receive_analytics(analytics_data: dict = Body(...)):
#async def receive_analytics(analytics_req : AnalyticsRequest):
    bandit_master.stats_keeper.analytics_backlog.append(analytics_data)
    print(bandit_master.stats_keeper.analytics_backlog)
    return {"mesage": "Received"}



@app.post("/add_job", status_code = 201) # 201 created
async def add_job_service( ajr : AddJobRequest, authorized: bool = Depends(verify_user)):
    resp = h3worker.add_job(ajr.id_, (ajr.lat,ajr.lng))
    return {"message": resp}

@app.post("/find_jobs")
async def find_job_service(fjr : FindJobRequest, authorized: bool = Depends(verify_user)):
    resp = h3worker.find_jobs((fjr.lat,fjr.lng))
    return {"job_ids": resp}

@app.post("/edit_job")
async def edit_job_service(ejr : EditJobRequest, authorized: bool = Depends(verify_user)):
    resp = h3worker.edit_job_loc(ejr.id_, (ejr.lat_old,ejr.lng_old), (ejr.lat_new,ejr.lng_new) )  ## For checking, dont go into redis. Instead, call (find_job) and check on py if it exists. Throw the error this scope.
    return {"message": resp}

@app.post("/del_job")
async def del_job_service(djr : DelJobRequest, authorized: bool = Depends(verify_user)):
    resp = h3worker.del_job_loc(djr.id_, (djr.lat,djr.lng))  ## For checking, dont go into redis. Instead, call (find_job) and check on py if it exists. Throw the error this scope.
    return {"message": resp}

@app.get("/")
async def home(authorized: bool = Depends(verify_user)):
     if authorized:
        return {"detail": "Authorised"}


    

