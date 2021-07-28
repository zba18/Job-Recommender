from collections import defaultdict
import time
import sqlite3
import json
import itertools
import pandas as pd
import numpy as np

import sqlalchemy
from sqlalchemy.sql import text
import traceback

class BanditMaster:
           
    def __init__(self, stats_keeper, demo = True):
        self.stats_keeper = stats_keeper
        self.demo = demo
        #defaults - blank
        self.bandit_master_state = {'feeds':  ## rename this?
            {
            'all':{'impressions':0, 'applies':0, 'views':0, 'saves':0},
            'click/impression':{'impressions':0, 'applies':0, 'views':0, 'saves':0},
            'applies/impression':{'impressions':0, 'applies':0, 'views':0, 'saves':0},
            'hired/applied':{'impressions':0, 'applies':0, 'views':0, 'saves':0},
            #'3':{'impressions':0, 'applies':0, 'views':0, 'saves':0},   
            },
            'latest_records':
                {'saves':0, 'applies':0, 'hires':0}
            }
        self.feed_decisions_list = []
        self.feed_decisions_generator = None
        
        self.load_bandit_master_from_db()
        self.generate_feed_decisions()
        
    #redundant function atm - ignore 
    def init_db_connections(self):
        self.local_db = sqlite3.connect('analytics.db')    # backups?
        #self.main_api_db = ''

        #ensure tables exist ?
        feed_stats_exists = bool( self.local_db.execute("""SELECT name FROM sqlite_master WHERE type='table' AND name='feed_stats';""").fetchone())                             
        return feed_stats_exists

    def load_bandit_master_from_db(self):
        #get latest json containing state of feed_stats_dict 
        state_json = self.stats_keeper.local_db.execute('SELECT vals FROM feed_stats WHERE id = (SELECT MAX(id) FROM feed_stats);').fetchone() # The table has to have an id prmary key
        if state_json:
            self.bandit_master_state = json.loads(state_json[0]) 
            print(self.bandit_master_state)

    def save_state_to_db(self):
        state_json = json.dumps(self.bandit_master_state)
        self.stats_keeper.local_db.execute('insert into feed_stats(time,vals) values (?,?)',(time.time_ns()), state_json)
        
    def generate_feed_decisions(self):
        self.keys2feed_names = [] 
        alphas = [] # one set of (alpha, beta) for each feed. these are for Thompson sampling. 
        betas = []
        
        for key, val in self.bandit_master_state['feeds'].items():
            if key != 'all':  # all feed is not a feed that needs to be recommended. (note: stats are still tracked in `process_analytics`, just for reference )
                self.keys2feed_names.append(key)
                im, ap = val['impressions'], val['applies'] # using apply/impression because POs said this is the driving stat. views/saves could be an alternative in future?
                alphas.append(ap + 1) #successes + 1
                betas.append(im-ap + 1) #fails + 1
            
        if not alphas:
            self.feed_decisions_list = list(range(len(self.keys2feed_names) - 1 )) #Dont know when this would happen... -1 to exclude 'all' feed
        else:
            beta_samples = np.random.beta(alphas, betas, size = (500, len(alphas)))
            optimal_feed = np.argmax(beta_samples, axis = 1)
            self.feed_decisions_list = list(optimal_feed)

        self.feed_decisions_generator = itertools.chain.from_iterable(itertools.repeat(self.feed_decisions_list))
        print('Feeds regenerated')
        
    #THIS FUNCTION IS IN BANDIT MASTER AS OPPOSED TO STATS KEEPER BECAUSE WE NEED FEED_STATS_DICT 
    def process_analytics(self):
        '''
        Work through backlog of json analytics
        Add those changes into the live working variables
        Then save into persisted storage (ad_stats and feed_stats table in local_db)
        '''

        try:

            #First, ingest the data
            print('Analytics processing...')
            if not self.stats_keeper.analytics_backlog:
                print('Empty backlog')
                
            # we track impressions (only), since views, saves, and applies are stored in the main behavior tables.
            # this is also only an approx. because impression tracking is currently only on app AFAIK.
            else:
                impressions = defaultdict(int) 
                print(self.bandit_master_state)

                for session_dict in self.stats_keeper.analytics_backlog: # check this this is the correct shape that is injested.
                    for feed_name, feed_dict in session_dict['feeds'].items():
                        for job_id in feed_dict['impressions']:
                            impressions[job_id] += 1

                        self.bandit_master_state['feeds'][feed_name]['impressions'] += len(feed_dict['impressions'])
                        self.bandit_master_state['feeds'][feed_name]['views'] += len(feed_dict['views'])
                        self.bandit_master_state['feeds'][feed_name]['saves'] += len(feed_dict['saves'])
                        self.bandit_master_state['feeds'][feed_name]['applies'] += len(feed_dict['applies'])
                        
                print(self.bandit_master_state)

                impressions_df = pd.Series(impressions)

            # Now check remote db for changes.
            # To prevent constantly downloading the multiple tables entirely, keep track of the latest IDS. 
            latest_retrieved_behaviors = self.bandit_master_state.get('latest_records', []) 
                    
            if latest_retrieved_behaviors:
                #one latest_record_id for each table: saves, applies, hires.
                s, a, h = latest_retrieved_behaviors['saves'], latest_retrieved_behaviors['applies'], latest_retrieved_behaviors['hires']

                queries = { 
                # for main db, modify these to WHERE id > latest instead of created_at >
                    'saves': text('SELECT user_id, gig_advertisement_id FROM gig_application_saves WHERE created_at > :latest').bindparams(latest= s ), #should be gig_advertisement_saves in maindb
                    'applies': text('SELECT user_id, gig_advertisement_id FROM gig_applications WHERE created_at > :latest').bindparams(latest= a ),
                    'hires': text('SELECT user_id, gig_advertisement_id FROM gig_hires WHERE created_at > :latest').bindparams(latest= h ), 
                }


                saves_df = pd.read_sql_query(queries['saves'], self.stats_keeper.remote_db_con,
                # index_col='id' #dummy table has no id column, will automatically be added by pd. Otherwise, this line is needed. NO I DONT THINK SO ANYMORE.
                ) 
                applies_df = pd.read_sql_query(queries['applies'], self.stats_keeper.remote_db_con,) 
                hires_df = pd.read_sql_query(queries['hires'], self.stats_keeper.remote_db_con, )
                

            else:

                saves_df = pd.read_sql_query('SELECT * FROM gig_saves;', self.stats_keeper.remote_db_con, )
                applies_df = pd.read_sql_query('SELECT * FROM gig_applications;', self.stats_keeper.remote_db_con, )
                hires_df = pd.read_sql_query('SELECT * FROM gig_hires;', self.stats_keeper.remote_db_con, )
            
            #update latest records
            #self.bandit_master_state['latest_records'] = {'saves': saves_df.index[-1],'applies': applies_df.index[-1],'hires': hires_df.index[-1]}
            
            # When the format [ad_id, user_id], aggregating user_id in using count gives the number of times it was saved/applied/hired
            #PIVOT TABLE INDEX SHOULD BE ON INDEX

            saves_counts_df = saves_df.pivot_table(index='gig_advertisement_id',values='user_id', aggfunc='count',)#.sort_values(by='user_id') 
            applies_counts_df = applies_df.pivot_table(index='gig_advertisement_id',values='user_id', aggfunc='count',) 
            hires_counts_df = hires_df.pivot_table(index='gig_advertisement_id',values='user_id', aggfunc='count',)

            # viewcount is a column in main db so it needs to be accessed directly 
            view_counts_df = pd.read_sql_query('SELECT id, view_count FROM gig_advertisements', self.stats_keeper.remote_db_con, index_col='id') # in main, change id to gig_advertisement_id

            ## all data is now downloaded. Proceed to update the table

            # Add views column first, this will ensure all new ads are present in ad_stats_df
            self.stats_keeper.ad_stats_df['views'] = view_counts_df['view_count'] 
            
            #merge data into ad_stats_df
            self.stats_keeper.ad_stats_df['impressions'] = self.stats_keeper.ad_stats_df['impressions'].add(impressions_df, fill_value = 0).fillna(0).astype(np.int32)
            self.stats_keeper.ad_stats_df['saves'] = self.stats_keeper.ad_stats_df['saves'].add(saves_counts_df['user_id'], fill_value = 0).fillna(0).astype(np.int32)
            self.stats_keeper.ad_stats_df['applies'] = self.stats_keeper.ad_stats_df['applies'].add(applies_counts_df['user_id'], fill_value = 0).fillna(0).astype(np.int32)
            self.stats_keeper.ad_stats_df['hires'] = self.stats_keeper.ad_stats_df['hires'].add(hires_counts_df['user_id'], fill_value = 0).fillna(0).astype(np.int32)
            
            self.stats_keeper.recompute_derived_stats()
            
            print(self.stats_keeper.ad_stats_df)
            print(self.bandit_master_state)

            if not self.demo:
                self.stats_keeper.save_ad_stats_to_db() 
                self.save_state_to_db()

            self.stats_keeper.analytics_backlog = []
            print('Analytics processed.')
            self.generate_feed_decisions()

        except Exception as e:
            print(traceback.format_exc())
            raise e
       
        
                
class StatsKeeper:
        
    def __init__(self, local_db_path = 'analytics.db', remote_db_eng = 'fake_main_behaviors_new.db' ):
        self.local_db_path = local_db_path
        self.local_db = sqlalchemy.create_engine(f"sqlite:///{self.local_db_path}")  # backups?
        
        if remote_db_eng == 'fake_main_behaviors_new.db':
            print('using simulated behaviors data from fake_main_behaviors_new')
            self.remote_db_con = sqlalchemy.create_engine(f"sqlite:///fake_main_behaviors_new.db")
        else:
            self.remote_db_con = remote_db_eng       
        
        #self.check_tables_exist()
        self.load_ad_stats_from_db() # populates self.ad_stats_df
        self.analytics_backlog = [] 
        

    def check_tables_exist(self):
        if self.local_db.execute("""SELECT name FROM sqlite_master WHERE type='table' AND name='ad_stats';""").fetchone():
            return
        else:
            raise Exception(f"ad_stats does not exist in {self.local_db_path}")
        
    
    def load_ad_stats_from_db(self):
        ##TO CHECK LATER: THAT INDEX OF AD_STATS matches job_id
        print(pd.read_sql_query('SELECT * FROM ad_stats', self.local_db))
        self.ad_stats_df = pd.read_sql_query('SELECT * FROM ad_stats', self.local_db,
         index_col='index' 
        )

    def save_ad_stats_to_db(self):
        self.ad_stats_df.to_sql('ad_stats',self.local_db, index=True, if_exists = 'replace')
         # backups? how?

    def recompute_derived_stats(self):
        stats_df = self.ad_stats_df
        
        # THESE ARE SOMEWHAT INACCURATE, SINCE IMPRESSIONS ARE ONLY TRACKED ON MOBILE. 
        # IF WE ASSUME MOBILE TO BE A RANDOM SAMPLE OF MOBILE+WEB, THESE NUMBERS ARE STILL VALID.
        # THOUGH THEYRE SLIGHTLY INFLATED BECAUSE MOBILE IMPRESSIONS < MOBILE+WEB IMPRESSIONS
        
        stats_df['click/impression'] = stats_df['views']/stats_df['impressions'] #click through rate
        stats_df['applies/impression'] = stats_df['applies']/stats_df['impressions'] # application rate
        stats_df['hired/applied'] = stats_df['applies']/stats_df['impressions'] # acceptentance rate (don't miss out!)
        stats_df['neg_hired/applied'] = -stats_df['hired/applied'] # reverse acceptance rate (be the early bird)
        stats_df.replace([np.inf, -np.inf, np.nan], -1, inplace=True)
        
        self.ad_stats_df = stats_df
        