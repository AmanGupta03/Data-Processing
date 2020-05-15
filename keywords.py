from sklearn.neighbors import NearestNeighbors
from globaldata import get_all_vectors 
from collections import OrderedDict

def get_all_keywords(centroids):
  final_dict={}
  for i in range(100):
    num_of_neigh=10
    common_dict={}

    urlnvect=get_all_vectors(i)
    url_list=urlnvect['urls']
    vector_list=urlnvect['embedding']
    
    if len(vector_list)<10:
      num_of_neigh=len(vector_list)
    neigh = NearestNeighbors(num_of_neigh)
    neigh.fit(vector_list)

    index_list = neigh.kneighbors([centroids[i]], num_of_neigh, return_distance=False)
    
    for indexes in index_list[0]:
      word_dict=getKeywordDict(url_list[indexes])
      tocommondict(word_dict,common_dict)
    final_dict[i]=final_words(common_dict)
  return final_dict

def getKeywordDict(url):
    cursor = cur.execute("SELECT content from global_data where url='"+str(url)+"'")
    for row in cursor:
      new_dict = { w.split(':')[0] : int(w.split(':')[1]) for w in row[0].split()}
    return new_dict

def tocommondict(word_dict,common_dict):
  for key in word_dict:
    if key not in common_dict:
      common_dict[key]=word_dict[key]
    else:
      common_dict[key]+=word_dict[key]

def final_words(common_dict):
  cluster_keywords={k: v for k, v in sorted(common_dict.items(), key=lambda item: item[1],reverse=True)}
  res = list(cluster_keywords.keys())[:10]
  listToStr =  ' '.join(res)
  return listToStr  