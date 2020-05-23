import sqlite3
from tqdm import tqdm
from sys import getsizeof as sizeof
from time import sleep
import pickle
from pymongo import MongoClient
#import pickle
import time

conn = sqlite3.connect("web.db")
cur = conn.cursor()



#query="select cluster from global_data where cluster not between 0 AND 99 "
#query="select cluster  FROM global_data where cluster IS NULL"
#query="delete  from global_data where cluster IS NULL"
#query ="slect url 
#cur.execute(query)
#conn.commit()

cursor=cur.execute("select cluster from global_data")

rows=cursor.fetchall()
print(rows[-5:-1])
print(len(rows))
conn.close()
#niprint(len(rows))
#now_mongo(dicti)
#print("mongo done")


#modelg = load_obj("../Dump_obj/kmeans")
#sleep(5)
'''
query = "SELECT url, "
for i in range(50):
  query += 'emb_d' + str(i)
  if(i != 49): query += ', '
query += ' FROM global_data '
'''

#cl_no = int(input("clusterno"))
#cursor = conn.execute("SELECT url,cluster_no from data")
#cursor = conn.execute("SELECT c92 from keywords where date_p=?",("2020-04-08",))
#cursor = conn.execute("SELECT url from data where cluster_no=?",(cl_no,))
#cursor = conn.execute("SELECT cluster_no,url from data ")
'''
dictt1={}
for row in cursor:
#for i in kmeans.labels_:
  if row[1] not in dictt1:
    dictt1[row[1]]=[]
  dictt1[row[1]].append(row[0])
for i in dictt1:
  ccursor = conn.execute("SELECT c"+str(i)+" from keywords where date_p=?",("2020-04-08",))
  for row in ccursor:
    print(i,"*****" ,row[0])
    print(dictt1[i][:20],"\n\n\n")
'''
