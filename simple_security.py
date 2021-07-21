import time
import hashlib
import cachetools
from fastapi import FastAPI, Request, HTTPException
import os
from dotenv import load_dotenv

load_dotenv()
secret = os.getenv('SECRET_KEY')
print(f"secret:{secret}")

salt_cache = cachetools.TTLCache(10000, 5)

async def verify_user(req: Request):  
    try:
        time_token = int(req.headers["time-token"])
        h = req.headers["auth-key"]
    except KeyError:
        raise HTTPException(status_code=401, detail="Unauthorized 1")
    except TypeError:
        raise HTTPException(status_code=412, detail="Precondition Failed") 
        
    #no reusing old tokens
    if not (time.time_ns() - 5 * 1000000000 < time_token < time.time_ns()): 
        raise HTTPException(status_code=401, detail="Unauthorized 2")
        
    #no using tokens more than once
    if salt_cache.get(time_token, None): # cache return None if hash is fresh, so disallow when True
        raise HTTPException(status_code=401, detail="Unauthorized 3")
    
    #confirm auth: test integrity of hash
    valid_h = hashlib.new('sha512')
    valid_h.update(f"{time_token}-{secret}".encode())
    
    if h != valid_h.hexdigest():
        print(h)
        print(valid_h.hexdigest())
        raise HTTPException(status_code=401, detail="Unauthorized 4")
        
    else: 
        salt_cache[time_token] = True #no using tokens more than once
        return True
    