import sqlite3
def get_keyword_dict(url):
    """ return dictionary of keywords of url """
    try:
      conn = sqlite3.connect('server/database/web.db') 
      cur = conn.cursor()
      cursor = cur.execute('SELECT content from global_data where url=?', (url,))
      
      for row in cursor:
        new_dict = { w.split(':')[0] : int(w.split(':')[1]) for w in row[0].split()}
      return new_dict

    except sqlite3.Error as error:
      print(error)
    
    finally:
      if (conn): conn.close()

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

def getTempKeywords(urls):
  for url in urls:
    common_dict={}
    word_dict=get_keyword_dict(url)
    tocommondict(word_dict,common_dict)  
  final_dict=final_words(common_dict)
  return final_dict.split()
