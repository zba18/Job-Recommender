import redis
import sqlalchemy

import pandas as pd
import numpy as np

from h3_redis import H3RedisWorker
from bandit_master import BanditMaster, StatsKeeper

from dotenv import load_dotenv
import os

## connect to all DBs

# Connection to localhost redis
redis_con = redis.Redis('localhost', port= 6379) # make sure this is running, port default is 6379
h3worker = H3RedisWorker(redis_con)

# Connection to main db
db_url = ''
db_port = 3306
username = ''
db_name = 'dev-adnexio'

load_dotenv()
password = os.getenv('STAGINGPASSWORD')

main_api_db = sqlalchemy.create_engine( f'mysql://{username}:{password}@{db_url}:{db_port}/{db_name}' )

stats_keeper = StatsKeeper(
    local_db_path = 'analytics.db', # this is SQLite
    remote_db_eng = main_api_db
    )

bandit_master = BanditMaster(stats_keeper)




# Clear all

# Clear Redis
for key in redis_con.scan_iter("prefix:*"):
    redis_con.delete(key)
    

# Clear ad_stats and feed_stats
ad_stats_exists = bool(stats_keeper.local_db.execute("""SELECT name FROM sqlite_master WHERE type='table' AND name='ad_stats';""").fetchone())
feed_stats_exists = bool(stats_keeper.local_db.execute("""SELECT name FROM sqlite_master WHERE type='table' AND name='feed_stats';""").fetchone())

if ad_stats_exists and feed_stats_exists:

    inp = input('Confirm reset ad_stats and feed_stats? Analytics session data stored will be deleted\n[y/n]').strip()
    if inp != 'y':
        print('aborted')
        exit()
    else:
        stats_keeper.local_db.execute('DROP TABLE ad_stats')
        stats_keeper.local_db.execute('DROP TABLE feed_stats')
#else:
#    raise FileNotFoundError 



# Populate data

# Begin by downloading data to populate ad_stats
saves_df = pd.read_sql_query('SELECT * FROM gig_advertisements_saves;', stats_keeper.remote_db_con, )
applies_df = pd.read_sql_query('SELECT * FROM gig_applications;', stats_keeper.remote_db_con, )
hires_df = pd.read_sql_query('SELECT * FROM gig_hires;', stats_keeper.remote_db_con, ) ## add gig_past_hires

latest_records = {'saves':0, 'applies':0,'hires':0}
try:
    latest_records['saves'] = saves_df.index.max()
    latest_records['applies'] = applies_df.index.max()
    latest_records['hires'] = hires_df.index.max()
except ValueError: #empty dataframes
    pass

saves_counts_df = saves_df.pivot_table(index='gig_advertisement_id',values='user_id', aggfunc='count',)#.sort_values(by='user_id') 
applies_counts_df = applies_df.pivot_table(index='gig_advertisement_id',values='user_id', aggfunc='count',) 
hires_counts_df = hires_df.pivot_table(index='gig_advertisement_id',values='user_id', aggfunc='count',)

view_counts_df = pd.read_sql_query('SELECT id, view_count FROM gig_advertisements',  stats_keeper.remote_db_con, index_col='id') # in main, change id to gig_advertisement_id

ads_lat_lng_df = pd.read_sql_query('SELECT id, latitude, longitude, FROM gig_advertisements',  stats_keeper.remote_db_con) 

## all data is now downloaded. Proceed to populate the tables

# populate redis.
ads_lat_lng_df.apply(lambda row: h3worker.add_job( row['id'], (row['latitude'], row['longitude']) ) ) # will probaly fail if coords are null.

#merge data combined above into ad_stats_df
stats_keeper.ad_stats_df = pd.DataFrame()
# Add views column first, this will ensure all ads are present in ad_stats_df
stats_keeper.ad_stats_df['views'] = view_counts_df['view_count'] 
stats_keeper.ad_stats_df['impressions'] =  0
stats_keeper.ad_stats_df['saves'] =  saves_counts_df['user_id'].astype(np.int32)
stats_keeper.ad_stats_df['applies'] = applies_counts_df['user_id'].astype(np.int32)
stats_keeper.ad_stats_df['hires'] =  hires_counts_df['user_id'].astype(np.int32)

stats_keeper.ad_stats_df.fillna(0,inplace=True)

stats_keeper.recompute_derived_stats()
stats_keeper.save_ad_stats_to_db() 

#populate feed_stats
## Start from scratch.
feed_names = ['all', 'view/impression', 'applies/impression','hired/applied'] # NOTE these should be linked to ad_stats columns as they are used as sort keys. Add them in stats_keeper.recompute_derived_stats()
initialized_feed_stats = { feed: {'impressions':0, 'applies':0, 'views':0, 'saves':0} for feed in feed_names }

initialized_state = {'feeds': initialized_feed_stats , 'latest_records': latest_records }
bandit_master.save_state_to_db()

print("\nMigration Successful\n\n")