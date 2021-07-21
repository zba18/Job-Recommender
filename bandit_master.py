from collections import defaultdict
import time
import sqlite3
import json
import itertools
import pandas as pd
import numpy as np

class BanditMaster:

    #$TO DO: DESIGNATE FEED 0 TO BE ALL FEED SOMEHOW?
    #TO DO: PUT PROJECT INTO GITPOD???
           
    def __init__(self, stats_keeper):
        self.stats_keeper = stats_keeper
        #defaults - blank
        self.feed_stats_dict = {'feeds':  ## rename this?
            {
            0:{'impressions':0, 'applies':0, 'view':0, 'save':0},
            1:{'impressions':0, 'applies':0, 'view':0, 'save':0},
            2:{'impressions':0, 'applies':0, 'view':0, 'save':0},
            3:{'impressions':0, 'applies':0, 'view':0, 'save':0},   
            },
            'latest_records':
                {'saves':0, 'applies':0, 'hires':0}
            }
        
        self.feed_decisions_list = []
        self.feed_decisions_generator = None
        
        self.load_bandit_master_from_db()
        self.generate_feed_decisions()
        

   #redundant function atm.     
    def init_db_connections(self):
        self.local_db = sqlite3.connect('analytics.db')    # backups?
        #self.main_api_db = ''

        #ensure tables exist 
        feed_stats_exists = bool( self.local_db.execute("""SELECT name FROM sqlite_master WHERE type='table' AND name='feed_stats';""").fetchone())                             
        return feed_stats_exists


    def load_bandit_master_from_db(self):
        #get latest json containing state of feed_stats_dict 
        state_json = self.stats_keeper.local_db.execute('SELECT vals FROM feed_stats WHERE id = (SELECT MAX(id) FROM feed_stats);').fetchone() # The table has to have an id prmary key
        if state_json:
            self.feed_stats_dict = json.loads(state_json)

        

    def save_bandit_master_to_db(self):
        state_json = json.dumps(self.feed_stats_dict)
        self.local_db.execute('insert into feed_stats(time,vals) values (?,?)',(time.time_ns()), state_json)
        self.local_db.commit()
        
        

    def generate_feed_decisions(self):
        self.keys2feed_names = [] # this should maybe be global?
        alphas = [] # one set of (alpha, beta) for each feed. these are for Thompson sampling. 
        betas = []
        
        for key, val in self.feed_stats_dict['feeds'].items():
            self.keys2feed_names.append(key)
            im, ap = val['impressions'], val['applies']
            alphas.append(ap + 1) #successes + 1
            betas.append(im-ap + 1) #fails + 1
            
        
        if not alphas:
            self.feed_decisions_list = list(range(len(self.keys2feed_names)))
        else:
            
            beta_samples = np.random.beta(alphas, betas, size = (500, len(alphas)))
            optimal_feed = np.argmax(beta_samples, axis = 0)
            self.feed_decisions_list = list(optimal_feed)

        self.feed_decisions_generator = itertools.repeat(self.feed_decisions_list)
        
        
    #THIS FUNCTION IS IN BANDIT MASTER AS OPPOSED TO STATS KEEPER BECAUSE WE NEED FEED_STATS_DICT 
    def process_analytics(self):
        '''
        Work through backlog of json analytics
        Add those changes into the live working variables
        Then save into persisted storage (ad_stats and feed_stats table in local_db)
        '''

        #First, ingest the data
        impressions = defaultdict(int)
        applies = defaultdict(int)
        views = defaultdict(int)
        saves = defaultdict(int)

        feed_imps = defaultdict(int)
        feed_apps = defaultdict(int)

        for session_dict in self.stats_keeper.analytics_backlog: # check this this is the correct shape that is injested.
            for feed in session_dict['feeds']:
                for job_id in feed['impressions']:
                    impressions[job_id] += 1

    #            for job_id in session['apply']:
    #                applies[job_id] += 1

    #            for job_id in session['view']:
    #                views[job_id] += 1

    #            for job_id in session['save']:
    #                saves[job_id] += 1

                self.feed_stats_dict[feed['feed']]['impressions'] += len(feed['impressions'])
                self.feed_stats_dict[feed['feed']]['view'] += -len(feed['view'])
                self.feed_stats_dict[feed['feed']]['save'] += len(feed['save'])
                self.feed_stats_dict[feed['feed']]['apply'] += len(feed['apply'])
                
        
        impressions_df = pd.DataFrame([impressions]).transpose()
        self.stats_keeper.ad_stats['impressions'] = self.stats_keeper.ad_stats['impressions'].add(impressions_df, fill_value = 0)
        # To prevent constantly downloading the multiple tables entirely, keep track of the latest IDS. 
        latest_retrieved_behaviors = self.feed_dict_stats.get('latest_records', []) 
        if latest_retrieved_behaviors:
            #one latest_record_id for each table: saves, applies, hires.
            s, a, h = latest_retrieved_behaviors['saves'], latest_retrieved_behaviors['applies'], latest_retrieved_behaviors['hires']

            saves_df = pd.read_sql_query(f'SELECT id, user_id, gig_advertisement_id FROM gig_advertisement_saves WHERE created_at > {s}', self.remote_db_con, index_col='id') # this needs to be safer
            applies_df = pd.read_sql_query(f'SELECT id, user_id, gig_advertisement_id FROM gig_applications WHERE created_at > {a}', self.remote_db_con, index_col='id') ## f'SELECT * FROM gig_applications WHERE id > {a}'
            hires_df = pd.read_sql_query(f'SELECT id, user_id, gig_advertisement_id FROM gig_hires WHERE created_at > {h}', self.remote_db_con, index_col='id')
        else:
            saves_df = pd.read_sql_query(f'SELECT * FROM gig_saves', self.remote_db_con, index_col='id') # this needs to be safer
            applies_df = pd.read_sql_query(f'SELECT * FROM gig_applications', self.remote_db_con, index_col='id')
            hires_df = pd.read_sql_query(f'SELECT * FROM gig_hires', self.remote_db_con, index_col='id')
        
        self.feed_dict_stats = {'saves': saves_df.index[-1],'applies': applies_df.index[-1],'hires': hires_df.index[-1]}
        
        
        saves_counts_df = saves_df.pivot_table(index='employer_id',values='user_id', aggfunc='count',)#.sort_values(by='user_id')
        applies_counts_df = applies_df.pivot_table(index='employer_id',values='user_id', aggfunc='count',)#.sort_values(by='user_id')
        hires_counts_df = hires_df.pivot_table(index='employer_id',values='user_id', aggfunc='count',)#.sort_values(by='user_id')
        
        # viewcount is a column in main db so it needs to be accessed directly
        view_counts_df = pd.read_sql_query('SELECT id, view_count FROM `gig_advertisements`', self.remote_db_con, index_col='id')
        
        
        #merge data into ad_stats
        self.stats_keeper.ad_stats['impressions'] = self.stats_keeper.ad_stats['impressions'].add(impressions_df, fill_value = 0)        
        self.stats_keeper.ad_stats['saves'] = self.stats_keeper.ad_stats['saves'].add(saves_counts_df, fill_value = 0)
        self.stats_keeper.ad_stats['applies'] = self.stats_keeper.ad_stats['applies'].add(applies_counts_df, fill_value = 0)
        self.stats_keeper.ad_stats['hires'] = self.stats_keeper.ad_stats['hires'].add(hires_counts_df, fill_value = 0)
        
        self.stats_keeper.ad_stats['views'] = view_counts_df ##  align by index first??!!!
        
        self.stats_keeper.recompute_derived_stats()
        
        
        self.stats_keeper.save_ad_stats_to_db()
        self.save_bandit_master_to_db()
        
        
        
        
        
                
class StatsKeeper:
        
    def __init__(self, local_db_path = 'analytics.db', remote_db_con = 'fake_main_behaviors_new.db' ):
        self.local_db_path = local_db_path
        self.local_db = sqlite3.connect(self.local_db_path)  # backups?
        self.remote_db_con = remote_db_con
        if remote_db_con == 'fake_main_behaviors_new.db':
            print('using simulated behaviors data from fake_main_behaviors_new')
            self.remote_db_con = sqlite3.connect('fake_main_behaviors_new.db')
                
        #self.check_tables_exist()
        self.load_ad_stats_from_db() # populates self.ad_stats_df
        self.analytics_backlog = [] 

    def check_tables_exist(self):
        if self.local_db.execute("""SELECT name FROM sqlite_master WHERE type='table' AND name='ad_stats';""").fetchone():
            return
        else:
            raise Exception(f"ad_stats does not exist in {self.local_db_path}")
        
        return ad_stats_exists
    
    def load_ad_stats_from_db(self):
        ##TO CHECK LATER: THAT INDEX OF AD_STATS matches job_id
        self.ad_stats_df = pd.read_sql_query('SELECT * FROM ad_stats', self.local_db)
        
    def save_ad_stats_to_db(self):
        self.ad_stats_df.to_sql('ad_stats',self.local_db, index=False, if_exists = 'replace')
                
        pass # backups? how?

    def recompute_derived_stats(self):
        stats_df = self.ad_stats_df
        
        # THESE ARE SOMEWHAT INACCURATE, SINCE IMPRESSIONS ARE ONLY TRACKED ON MOBILE. 
        # IF WE ASSUME MOBILE TO BE A RANDOM SAMPLE OF MOBILE+WEB, THESE NUMBERS ARE STILL VALID.
        # THOUGH THEYRE SLIGHTLY INFLATED BECAUSE MOBILE IMPRESSIONS < MOBILE+WEB IMPRESSIONS
        
        stats_df['click/impression'] = stats_df['views']/stats_df['impressions'] #click through rate
        stats_df['applies/impression'] = stats_df['applies']/stats_df['impressions'] # application rate
        stats_df['hired/applied'] = stats_df['applies']/stats_df['impressions'] # acceptentance rate (don't miss out!)
        stats_df['neg_hired/applied'] = -stats_df['hired/applied'] # reverse acceptance rate (be the early bird)
        stats_df.replace(np.inf, -1, inplace=True)
        
        self.ad_stats_df = stats_df
        
        
        

        
        

