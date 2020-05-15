""" consist functions to update and fetch data from global_data table """

from datetime import date, timedelta
from settings import DB_PATH
from tqdm import tqdm
import sqlite3

def add_new_records(data):
  """ Append new records in global_data """
  
  try:
    conn = sqlite3.connect(DB_PATH) 
    data.to_sql('global_data', conn, if_exists='append', index=True)
    conn.commit()
    print("Successfully added new records")
  except sqlite3.Error as error:
    print("Error while adding new records in global_data ", error)
  finally:
    if (conn): conn.close()


def delete_records(cur_date, duration=30):
  """ Delete all records that doesn't enter in ranklist for given duration """

  expired = str(cur_date-timedelta(days=duration))

  try:
    conn = sqlite3.connect(DB_PATH) 
    cur = conn.cursor()
    cur.execute("SELECT url, cluster FROM global_data WHERE date = ?", (expired,))
    rows = cur.fetchall()
    cur.execute("DELETE FROM global_data WHERE date = ?", (expired,))
    print("Successfully removed", len(rows), 'entry')
    conn.commit()
    return rows
  except sqlite3.Error as error:
    print("Error while deleting records in global_data", error)
  finally:
    if (conn): conn.close()


def update_rank(ranks):
  """ update rank_d1 to rank_d30 in global_data """

  query = "UPDATE global_data SET "
  for i in range(1, 30):
    query += "rank_d" + str(i) + " = " + "rank_d" + str(i+1)
    if i != 29: query += ", "

  try:
    conn = sqlite3.connect(DB_PATH) 
    cur = conn.cursor()
    cur.execute(query)
    cur.execute('UPDATE global_data SET rank_d30=NULL')

    for row in tqdm(ranks):
      cur.execute('UPDATE global_data SET rank_d30=? WHERE url=?', (row[1], row[0]))
    
    print("Successfully update ranks")
    conn.commit()
  except sqlite3.Error as error:
    print(error)
  finally:
    if (conn): conn.close()


def update_cluster(clusters):
  """ update cluster no of new_domains in global_data """

  try:
    conn = sqlite3.connect(DB_PATH) 
    cur = conn.cursor()
    
    for row in tqdm(clusters):
      cur.execute('UPDATE global_data SET cluster=? WHERE url=?', (row[1], row[0]))

    conn.commit()
    print("Successfully update cluster no.")

  except sqlite3.Error as error:
    print(error)
  finally:
    if (conn): conn.close()



def update_date(urls, cur_date):
  """ update date of all domains global_data"""

  try:
    conn = sqlite3.connect(DB_PATH) 
    cur = conn.cursor()
    
    for url in tqdm(urls):
      cur.execute('UPDATE global_data SET date=? WHERE url=?', (cur_date, url))

    conn.commit()
    print("Successfully update date")

  except sqlite3.Error as error:
    print(error)
  finally:
    if (conn): conn.close()



def get_rank_cluster():
  """ return cluster and rank of all urls in global_data """
  try:
    conn = sqlite3.connect(DB_PATH) 
    cur = conn.cursor()
    cur.execute("SELECT cluster,rank_d30 FROM global_data")
    return cur.fetchall();

  except sqlite3.Error as error:
    print(error)

  finally:
    if (conn): conn.close()


def get_keyword_dict(url):
    """ return dictionary of keywords of url """
    try:
      conn = sqlite3.connect(DB_PATH) 
      cur = conn.cursor()
      cursor = cur.execute('SELECT content from global_data where url=?', (url,))
      
      for row in cursor:
        new_dict = { w.split(':')[0] : int(w.split(':')[1]) for w in row[0].split()}
      return new_dict

    except sqlite3.Error as error:
      print(error)
    
    finally:
      if (conn): conn.close()


def get_all_vectors(cluster=None):
  """ return  dictionary consist list of urls, and embeddings of given cluster, 
      in case of None it return vectors of all cluster """

  query = "SELECT url, "
  for i in range(50):
    query += 'emb_d' + str(i)
    if(i != 49): query += ', '
  query += ' FROM global_data '

  if(cluster is not None): query += 'WHERE cluster='+str(cluster)

  try:
    
    conn = sqlite3.connect(DB_PATH) 
    cur = conn.cursor()
    cursor = cur.execute(query)

    urls, embedding = [], []
    
    for row in cursor:
      urls.append(row[0])
      embedding.append(row[1:])
    
    return dict({'embedding':embedding,'urls':urls})

  except sqlite3.Error as error:
    print(error)

  finally:
    if (conn): conn.close()