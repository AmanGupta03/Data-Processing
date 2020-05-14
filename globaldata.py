""" consist functions to update and fetch data from global_data table """

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