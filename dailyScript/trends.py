from keywords import get_all_keywords
from sklearn import cluster, metrics
from collections import defaultdict
from insert import insertData
from settings import KMEANS_PATH
import globaldata
import pickle

def load_obj(filename):
  with open(filename,'rb') as obj_file:
    obj = pickle.load(obj_file)
    return obj


def store_object(obj,filename):
  with open(filename,'wb') as obj_file:
    pickle.dump(obj,obj_file)

kmeans=load_obj(KMEANS_PATH)



def getRankSizeData():

  sizeRankDictList = []
  sizeDict=defaultdict(int)
  rankDict=defaultdict(int)
  
  data = globaldata.get_rank_cluster()

  for row in data:
    sizeDict[row[0]]+=1
    if row[1]==None:
      rankDict[row[0]]+=200000
    else:
     rankDict[row[0]]+=row[1]
  
  sizeRankDictList.append(sizeDict)
  sizeRankDictList.append(rankDict)
  
  return sizeRankDictList  



def update_trends(urls, X, date):

  cluster_no = []

  kmeans.partial_fit(X)
  cluster_no= kmeans.labels_
  centroids = kmeans.cluster_centers_
  
  globaldata.update_cluster(list(zip(urls, cluster_no)))
  
  print('Updating SIZE...')
  rankSizelist = getRankSizeData()
  insertData(rankSizelist[0],'SIZE',date)
  print('Updating RANK...')
  insertData(rankSizelist[1],'RANK',date)
  
  keywords_insert=get_all_keywords(centroids)
  print('Updating KEYWORDS...')
  insertData(keywords_insert,'KEYWORDS',date)
  
  store_object(kmeans,KMEANS_PATH)
  print('Trends Updated')
