""" 
    Exact time of running this script is still to find...
    Note-: cisco ranklist lags by 1 day 
"""

from datetime import date, timedelta
from time import sleep
import sys

behind = 1 #no-of days global data lags with current date... change according to date of running

if len(sys.argv) > 1: behind = int(sys.argv[1])
cur_date = date.today() - timedelta(days=behind) 
print('ALERT -:')

print('This will update db using cisco-ranklist at', cur_date, 
       "Terminate process if previous updates are pending,  and run py passing corrsponding value"
       "'behind' as command-line arguments")

sleep(2)

import newdomains
import globaldata
import trends


print('Fetching cisco ranklist...')
urls = newdomains.fetch_ranklist(cur_date)

print('Filtering cisco ranklist...')
valid_urls = newdomains.process_ranklist(urls)   
print(len(valid_urls), 'valid urls found')

print('Filtering Already visited domains...')
url_to_scrap = newdomains.get_url_to_scrap(valid_urls)
print('\n',len(url_to_scrap), 'new domains found')

max_url_limit = 20000

print('Finding active urls...')
new_active_urls = newdomains.get_active_urls(url_to_scrap[:max_url_limit]) #slice list while testing...approximately perform 10-30 it/s 
print('\n',len(new_active_urls), 'new active domains found')

print('\nScrapping urls...')
data = newdomains.fast_scrap_limited(new_active_urls, cur_date, limit=1) #approximately perform 5-10 it/s...
print('\n',len(data), 'new domains scrapped')

print('\nAdjusting ranks...')
ranks = newdomains.get_adjusted_ranks(cur_date, data['urls'], urls)

print('performing updates on global_data.....')

deleted_records = globaldata.delete_records(cur_date, duration=30) 
globaldata.update_rank(list(ranks.items()))
globaldata.update_date(list(ranks.keys()), str(cur_date))

print('updating trends.......')
trends.update_trends(data['urls'], data['embedding'], str(cur_date))

print('updating visited domains...')
newdomains.update_visited_domains(list(ranks.keys()), data['urls'], cur_date)

print('SUCCESS')
