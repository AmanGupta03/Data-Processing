from concurrent.futures import ProcessPoolExecutor, as_completed
from scrapper import get_valid_url, get_all_info
from datetime import date, timedelta
from tldextract import extract
from settings import DB_PATH
from tqdm import tqdm
import pandas as pd
import numpy as np
import globaldata
import sqlite3


MAX_LIMIT = 500000   #no of entries to consider from ranklist
CISCO_RANKLIST = 'http://s3-us-west-1.amazonaws.com/umbrella-static/top-1m-{date:}.csv.zip'


def is_valid(url):
  """ return true if url is domain or false if it is subdomain """
  
  if not url.startswith('http'): 
    url = 'https://'+ url

  try:
    subdomain = extract(url).subdomain
    if subdomain == "" or subdomain == "www": return True
    else: return False
  except:
    return False


def fetch_ranklist(date):
  """ fetch cisco ranklist of given date """

  df = pd.read_csv(CISCO_RANKLIST.format(date=date))
  urls = [row[1] for row in df.values.tolist()] 
  urls.insert(0, df.columns[1])
  return urls


def get_visited_domains():
  """ return set of visited domains fetched from database """

  try:
    conn = sqlite3.connect(DB_PATH) 
    cur = conn.cursor()
    cur.execute('SELECT url From visited_domains')
    return set([extract(data[0]).domain for data in cur.fetchall()])
  except sqlite3.Error as error:
    print(error)
  finally:
    if (conn): conn.close()


def get_all_fetched_domains(cur_date):
  """ return set of all domains that are also in global_data """
  expired = str(cur_date-timedelta(days=30))
  try:
    conn = sqlite3.connect(DB_PATH) 
    cur = conn.cursor()
    cur.execute('SELECT url From visited_domains WHERE status = 1 AND date != ?', (expired,))
    return [data[0] for data in cur.fetchall()]
  except sqlite3.Error as error:
    print(error)
  finally:
    if (conn): conn.close()


def process_ranklist(urls, entries=MAX_LIMIT):
  """ remove subdomain/invalid domains from list 
      too lower rank websites will be removed"""
  
  valid_urls = list(filter(is_valid, urls[:entries]))
  return valid_urls


def get_url_to_scrap(urls):
  """ remove already visited domains and adjust format of urls """

  visited_domains = get_visited_domains()
  new_visited_domains = set()
  url_to_scrap = []

  for url in tqdm(urls):
    domain = extract(url).domain
    suffix = extract(url).suffix
    if domain not in visited_domains and domain not in new_visited_domains:
      new_visited_domains.add(domain)
      url_to_scrap.append(domain+'.'+suffix)
  
  return url_to_scrap


def get_active_urls(urls, workers=3):
  """ i) remove all url that doesn't respond or inactive, 
      ii) Add valid protocol to active urls """

  with ProcessPoolExecutor(max_workers=workers) as executor:
    futures = [executor.submit(get_valid_url, url) for url in urls]
    results = []
    for result in tqdm(as_completed(futures)):
      if result.result()['status'] == 200:
        results.append(result.result()['url'])

  return results


def get_adjusted_ranks(cur_date, new_urls, urls):
  """ return current_day ranklist of all urls in data """

  all_fetched_urls = set(get_all_fetched_domains(cur_date))

  for url in new_urls:
    all_fetched_urls.add(url)
  
  all_urls = process_ranklist(urls, entries=len(urls)) 
  
  present_domains = set()
  url_dict = {}
  
  # map domain name with their url 
  for url in tqdm(all_fetched_urls):
    domain = extract(url).domain
    present_domains.add(domain)
    url_dict[domain] = url

  # filter duplicates/inactive/unscrapped domains
  seen = set()
  adjusted_rank_list = []
  for url in tqdm(all_urls):
    domain = extract(url).domain
    if domain not in seen and domain in present_domains:
      adjusted_rank_list.append(url_dict[domain])
      seen.add(domain)

  ranks = {url:(i+1) for i,url in enumerate(adjusted_rank_list)}
  return ranks


def delete_visited_domain(cur_date, duration=30):
  """ delete all entries in visited_domains table that are 30days old """
  expired = str(cur_date-timedelta(days=duration))
  try:
    conn = sqlite3.connect(DB_PATH) 
    cur = conn.cursor()
    cur.execute("DELETE FROM visited_domains WHERE status=1 and date = ?", (expired,))
    conn.commit()
  except sqlite3.Error as error:
    print(error)
  finally:
    if (conn): conn.close()


def add_new_visited_domains(new_url, cur_date):
  """ Add new entries in visited_domains table, that found at cur_date """

  row = [[url, str(cur_date), 1] for url in new_url]
  df = pd.DataFrame(row, columns=['url', 'date', 'status'])
  df.set_index('url', inplace=True)

  try:
    conn = sqlite3.connect(DB_PATH) 
    df.to_sql('visited_domains', conn, if_exists='append', index=True)
    conn.commit()
  except sqlite3.Error as error:
    print("Error while adding new records in visited_domains", error)
  finally:
    if (conn): conn.close()
  

def update_visited_domains_date(urls, cur_date):
  """ update date column in visited_domains table """
  
  try:
    conn = sqlite3.connect(DB_PATH) 
    cur = conn.cursor()

    for url in tqdm(urls):
      cur.execute('UPDATE visited_domains SET date=? WHERE url=?', (cur_date, url))
    conn.commit()
  
  except sqlite3.Error as error:
    print(error)
  
  finally:
    if (conn): conn.close()
  


def update_visited_domains(all_url, new_url, cur_date, duration=30):
  """ Add all url seen at cur_date and delete url that doesn't encounter in last x days """
  
  delete_visited_domain(cur_date, duration)
  add_new_visited_domains(new_url, cur_date)
  update_visited_domains_date(all_url, str(cur_date))


def data_to_append(data, cur_date):
  """ return data in a format to append on database """
  
  for i in tqdm(range(len(data))):
    row = data[i]
    temp = [row['url'], str(cur_date), row['content']]
    temp.extend([float(x) for x in row['embedding'].split()])
    data[i] = temp
  
  default = [np.nan for i in range(31)]
  
  for row in data:
    row.extend(default)


  #getting column names
  columns = ['url', 'date', 'content']
  columns.extend(['emb_d'+str(i) for i in range(50)])
  columns.extend(['rank_d'+str(i+1) for i in range(30)])
  columns.append('cluster')

  #convert in dataframe
  df = pd.DataFrame(data, columns=columns)
  df.set_index('url', inplace=True)
  
  return df


def fast_scrap(urls, workers=3):
  """ Avoid dynamic content scrapping as webdriver cannot handle multiprocessing """

  with ProcessPoolExecutor(max_workers=workers) as executor:
    futures = [executor.submit(get_all_info, url) for url in urls]
    results = []
    for result in tqdm(as_completed(futures)):
      if result.result()['status'] == 'success':
        results.append(result.result())

  return results


def fast_scrap_limited(urls, cur_date, workers=3, limit=50):
  """ This will scrap only domaisn upto 'limit' in one go...use it to optimise ram uses """

  data = {'urls':[], 'embedding':[]}
  start_idx = 0
  phase = 0
  
  while(start_idx < len(urls)):
    temp = fast_scrap(urls[start_idx:start_idx+limit], workers=workers)
    print('phase', str(phase), 'scrapping completed')
    data['urls'].extend([row['url'] for row in temp])
    data['embedding'].extend([[float(x) for x in row['embedding'].split()] for row in temp] )
    temp = data_to_append(temp, cur_date)
    globaldata.add_new_records(temp) 
    start_idx += limit
    phase += 1

  return data


def complete_scrap(urls):
  """ Also perform dynamic content scrapping 
      Note -: approx 100 times slower than fast_scrap """
  
  for url in tqdm(urls):
    result = get_all_info(url)
    if result['status'] == 'success':
      results.append(result)
  
  return result
