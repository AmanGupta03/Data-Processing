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
new_active_urls = newdomains.get_active_urls(url_to_scrap[:30]) #slice list while testing...approximately perform 10-30 it/s 
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

print(new_scrapped_url)
print(embedding)
print(data)

print('performing initial updates on global_data.....')

"""

   Should also return 'url', 'cluster' as list of tuples of all the deleted entries
-> deleted_records = globaldata.delete_records(cur_date, duration=30) #note-: date are store as strings in db, while here pass as 'date' 

-> globaldata.add_new_records(data) #Appending new results on global_data

"""


print('updating trends.db......')

"""

IMP-:
*new_scrapped_url - list of all newl url:
*embedding -: exactly same as X, consist list of 50 dimensional embeddding of new_scrapped_url in floats
*deleted_records -: consist 'url', 'cluster' as list of tuples of all the deleted entries
*Rank of all deleted entries are obviously 'None'

Replace these dummy function calls with actual one in trends.py......

**it should return list of tuple consist 'url', 'cluster'...That's needed to update global_data
-> clusters = trends.refine_clustering(new_scrapped_url, embedding)

-> trends.update_cluster_ranks(ranks)

-> trends.update_top_keywords()

"""


print('update ranks, date and cluster in global_data.....')

"""

update rank_d1 to ranks_d30
params-: (url, rank_d30) as list of tuple 
-> globaldata.update_rank(list(ranks.items()))

#update cluster, where clusters is list of tuple (url, cluster_no)
-> globaldata.update_cluster(clusters)

#update_date of 'url' passed as list,  with 'cur_date' passed as string
-> global_data.update_date(list(ranks.keys()), str(cur_date))

"""

print('update visited domains...')
#Keep this section commented in test running

"""
print('update visited domains...')
new_domains.update_visited_domains(list(ranks.items()), new_scrapped_url)

"""