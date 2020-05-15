""" 
    Exact time of running this script is still to find...
    Note-: cisco ranklist lags by 1 day 
"""

from datetime import date, timedelta
import newdomains
import globaldata
import trends

behind = 8  #no-of days global data lags with current date... change according to date of running
cur_date = date.today() - timedelta(days=behind) 

print('Fetching cisco ranklist...')
urls = newdomains.fetch_ranklist(cur_date)

print('Filtering cisco ranklist...')
valid_urls = newdomains.process_ranklist(urls)   
print(len(valid_urls), 'valid urls found')

print('Filtering Already visited domains...')
url_to_scrap = newdomains.get_url_to_scrap(valid_urls)
print('\n',len(url_to_scrap), 'new domains found')

print('Finding active urls...')
new_active_urls = newdomains.get_active_urls(url_to_scrap) #slice list while testing...approximately perform 10-30 it/s 
print('\n',len(new_active_urls), 'new active domains found')

print('\nScrapping urls...')
data = newdomains.fast_scrap(new_active_urls) #approximately perform 5-10 it/s...
print('\n',len(data), 'new domains scrapped')

print('\nAdjusting ranks...')
ranks = newdomains.get_adjusted_ranks(cur_date, data, urls)

new_scrapped_url = [row['url'] for row in data] 
embedding = [[float(x) for x in row['embedding'].split()] for row in data] #embeddings as list of list, ordered according to new_scrapped url

#get pandas dataframe indexed on url to append on global_data
data = newdomains.data_to_append(data, cur_date) 


print('performing updates on global_data.....')

deleted_records = globaldata.delete_records(cur_date, duration=30) 
globaldata.add_new_records(data) 
globaldata.update_rank(list(ranks.items()))
globaldata.update_date(list(ranks.keys()), str(cur_date))

print('updating trends.......')
trends.update_trends(new_scrapped_url, embedding, date)

print('updating visited domains...')
newdomains.update_visited_domains(list(ranks.keys()), new_scrapped_url, cur_date)

print('SUCCESS')