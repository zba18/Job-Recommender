import redis

from fastapi import FastAPI, HTTPException, Depends
from models import FindJobRequest, AddJobRequest, EditJobRequest, DelJobRequest

from h3_redis import H3RedisWorker
import simple_security
from simple_security import verify_user


r = redis.Redis('localhost') # make sure this is running, port default is 6379

h3service = H3RedisWorker(r)

app = FastAPI()


@app.post("/add_job")
async def add_job_service( ajr : AddJobRequest, authorized: bool = Depends(verify_user)):
    resp = h3service.add_job(ajr.id_, (ajr.lat,ajr.lng))
    return {"message": resp}

@app.post("/find_jobs")
async def find_job_service(fjr : FindJobRequest, authorized: bool = Depends(verify_user)):
    resp = h3service.find_jobs((fjr.lat,fjr.lng))
    return {"job_ids": resp}

@app.post("/edit_job")
async def edit_job_service(ejr : EditJobRequest, authorized: bool = Depends(verify_user)):
    resp = h3service.edit_job_loc(ejr.id_, (ejr.lat_old,ejr.lng_old), (ejr.lat_new,ejr.lng_new) )  ## For checking, dont go into redis. Instead, call (find_job) and check on py if it exists. Throw the error this scope.
    return {"message": resp}

@app.post("/del_job")
async def del_job_service(djr : DelJobRequest, authorized: bool = Depends(verify_user)):
    resp = h3service.del_job_loc(djr.id_, (djr.lat,djr.lng))  ## For checking, dont go into redis. Instead, call (find_job) and check on py if it exists. Throw the error this scope.
    return {"message": resp}

@app.get("/")
async def home(authorized: bool = Depends(verify_user)):
     if authorized:
        return {"detail": "Authorised"}

