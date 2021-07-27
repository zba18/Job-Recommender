# Job Recommender + Fast Proximity API

## installation and startup

``` 

sudo add-apt-repository ppa:redislabs/redis
sudo apt-get update
sudo apt-get install -y redis

redis-server

sudo apt-get install -y python3.8
python -m venv venv -system-site-packages

source venv/bin/activate 
pip install -r "requirements.txt"
uvicorn main_rec:app

```

## Component Overview
### User:
1. Request feeds from backend
1. Receive sorted recommended ads and content from main backend
1. Relay user analytics directly to /submit_analytics endpoint (to be handled by stats keeper)

### Backend:
1. Finds nearby_ad_ids
1. Request nearby_ad_stats from stats keeper for sorting by metrics
1. Receive sorted_nearby_ads by sorting (bandit master)
1. Send sorted_nearby_ads to user

### Bandit Master: 
1. Use accumulated data to decide which feed should be shown.
1. Generate distributions and sample from them.

### Stats keeper:
1. Track and record high volume behavior data. 
1. Update ad_stats table at regular intervals.

## Recommended Feed full flow
### User:
1. User requests Recommended Rec Feed
1. Backend returns backend data.
1. Analytics Proximity API finds (<5 km) nearby_ad_list.
1. Bandit Master picks a feed to sort by
1. Bandit Master sorts nearby_ad_list to return ordered_rec_feed recommendations.
1. Backend receives recommendations ids 
1. User is shown Recommended Feed
1. User device relays impressions/views/saves/applies for each ad to behaviour tables

### Backend (detailed):
1. User requests ‘Recommended’
1. Request Recommended Rec Feed (Latitude, Longitude) from analytics API
1. Send to user content for the ids in ordered_rec_feed

### Bandit Master:
1. Backend requests recommendations for the user location
1. Run nearby ads (Proximity API) to get nearby_ad_list
1. Query ids in nearby_ad_list from ad_stats table to get nearby_ad_stats
1. Thompson sample the distributions to get the optimal feed. 
1. Sort nearby_ad_id list by feed
1. Send ordered_rec_feed to backend/user

(every X minutes)
1. When ad_stats table is updated by Stats Keeper, regenerate Thompson samples and optimal feeds to serve.

### Stats Keeper: 
(async)
1. Receive and store analytics stats

(every X minutes)
1. Update stats by:
1. Work through analytics backlog.
1. fetching new behaviors from main behavior tables
