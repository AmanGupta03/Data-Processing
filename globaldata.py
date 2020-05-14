""" consist functions to update and fetch data from global_data table """

from datetime import date, timedelta
import sqlite3

def add_new_records(data):
  """ Append new records in global_data """
  
  try:
    conn = sqlite3.connect("web.db") 
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
    conn = sqlite3.connect("web.db") 
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
    conn = sqlite3.connect("web.db") 
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
    conn = sqlite3.connect("web.db") 
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
    conn = sqlite3.connect("web.db") 
    cur = conn.cursor()
    
    for url in tqdm(urls):
      cur.execute('UPDATE global_data SET date=? WHERE url=?', (cur_date, url))

    conn.commit()
    print("Successfully update date")

  except sqlite3.Error as error:
    print(error)
  finally:
    if (conn): conn.close()