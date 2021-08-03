import pandas as pd
import sqlite3 as sl

con = sl.connect('analytics.db')
feed = pd.read_sql_query('select * from feed_stats', con)
ad = pd.read_sql_query('select * from ad_stats', con)

print(feed.tail())
print(ad.head())
print(ad.tail())