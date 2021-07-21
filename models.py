from pydantic import BaseModel
from typing import Optional, List

class FindJobRequest(BaseModel):
    lat: float
    lng: float
        
class AddJobRequest(BaseModel):
    id_: int 
    lat: float
    lng: float

class DelJobRequest(BaseModel):
    id_: int 
    lat: float
    lng: float
        
class EditJobRequest(BaseModel):
    id_: int 
    lat_old: float
    lng_old: float
    lat_new: float
    lng_new: float
        

class AnalyticsRequest(BaseModel):
	feed: int
	impressions: Optional[List[int]]
	views: Optional[List[int]]
	saves: Optional[List[int]]
	applies: Optional[List[int]]