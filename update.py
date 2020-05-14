""" 
    Exact time of running this script is still to find...
    Note-: cisco ranklist lags by 1 day 
"""

from datetime import date, timedelta
import newdomains
import globaldata
import trends

behind = 7  #no-of days global data lags with current date... change according to date of running
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
new_active_urls = newdomains.get_active_urls(url_to_scrap[:20]) #slice list while testing...approximately perform 10-30 it/s 
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


print('performing initial updates on global_data.....')

#deleted_records = globaldata.delete_records(cur_date, duration=30) 
#globaldata.add_new_records(data) 



print('updating trends.db......')

"""

IMP-:
*new_scrapped_url - list of all newl url:
*embedding -: exactly same as X, consist list of 50 dimensional embeddding of new_scrapped_url in floats
*ranks -: ranks of all url in global_data as dictionary
*deleted_records -: consist 'url', 'cluster' as list of tuples of all the deleted entries
*Rank of all deleted entries are obviously 'None'

Replace these dummy function calls with actual one in trends.py......

**it should return list of tuple consist 'url', 'cluster'...That's needed to update global_data
-> clusters = trends.refine_clustering(new_scrapped_url, embedding)

-> trends.update_cluster_ranks(ranks)

-> trends.update_top_keywords()

"""


print('update rank,  date and cluster in global_data.....')

#globaldata.update_rank(list(ranks.items()))
#globaldata.update_cluster(clusters)
#globaldata.update_date(list(ranks.keys()), str(cur_date))


print('update visited domains...')
newdomains.update_visited_domains(list(ranks.keys()), new_scrapped_url, cur_date)

print('SUCCESS')