""" Script to create initial database """

from tqdm import tqdm
import pandas as pd
import numpy as np
import sqlite3

DATASET_URL = 'https://ia601502.us.archive.org/10/items/global_dataset_2.3/global_dataset_2.3.zip'
VISITED_DOMAINS_URL = 'https://raw.githubusercontent.com/AmanGupta03/project-data/master/visited_domains.csv'

#ASSUMING initial cluster_no as list of tuples (url, cluster)
clusters = '???'


def build_global_data(clusters, dataset):
  """ create initial global_data table """

  data = pd.read_csv(dataset)
  data = data.values.tolist()

  for row in data:
    row.append(np.nan)

  columns = ['url', 'date', 'content', 'embedding']
  columns.extend(['rank_d'+str(i+1) for i in range(30)])
  columns.append('cluster')

  data = pd.DataFrame(data, columns=columns)
  data.set_index('url', inplace=True)

  conn = sqlite3.connect("web.db") 
  data.to_sql('global_data', conn, if_exists='replace', index=True)

  print('feeding cluster inf....')
  cur = conn.cursor()

  for row in tqdm(clusters):
    cur.execute('UPDATE global_data SET cluster=? WHERE url=?', (row[1], row[0]))

  conn.commit()
  conn.close()


def build_visited_domains(dataset):
  """ create initial visited domain table """
  
  conn = sqlite3.connect("web.db")
  df = pd.read_csv(dataset, index_col='url')
  df.to_sql('visited_domains', conn, if_exists='replace', index=True)
  conn.close()


#build_global_data(clusters=clusters, dataset=DATASET_URL)
build_visited_domains(dataset=VISITED_DOMAINS_URL)