import pandas as pd
import numpy as np
from flask import Flask
from flask_cors import CORS, cross_origin
import server.initialize
import json
import os
import urllib.parse
from server.get_cluster_info import getInfo
from server.helper import getClusterDataList
from datetime import date
from datetime import timedelta



ASSETS_DIR = os.path.dirname(os.path.abspath(__file__))
print("in app")

app = Flask(__name__)
from server.get_result import finalFunction
from server.search import search_by_query,search_by_domain,get_cluster_websites
cors = CORS(app)
app.config['CORS_HEADERS'] = '*'



@app.route('/<path:url>')
@app.route('/')
@cross_origin()
def result_url(url=''):
    print(url)
    return finalFunction(url,1)

@app.route('/<path:url>/<int:status>')
@app.route('/')
@cross_origin()
def result_url1(status,url=''):
#@app.route('/<path:url>')
#@app.route('/')
#@cross_origin()
#def result_url(url=''):
    print(url)
    return finalFunction(url,status)

@app.route('/getclusterurl/<int:cluster_no>/')
@app.route('/getclusterurl/')
@cross_origin()
def get_cluster_url(cluster_no=-1,page_no=1):
    if cluster_no <0 or cluster_no >100:
        return json.dumps(get_cluster_websites())
    else:
        return json.dumps(get_cluster_websites(cluster_no))

@app.route('/search/query/<q>/clusterno/<int:cluster_no>')
@app.route('/search/query/<q>')
@cross_origin()
def query_search(q,cluster_no=-1):
    q=urllib.parse.unquote(q).split(" ")
    print(q)
    return json.dumps(search_by_query(q,cluster_no))

@app.route('/search/domain/<q>')
@cross_origin()
def domain_search(q):
    q=urllib.parse.unquote(q).split(" ")
    return json.dumps(search_by_domain(q))

@app.route('/getinfo/')
@cross_origin()
def get_info_route():
    return json.dumps(getInfo())

@app.route('/getClusterData/<startDate>/<endDate>/<int:cluster_no>')
@cross_origin()
def getClusterData(startDate,endDate,cluster_no):
  return json.dumps(getClusterDataList(startDate,endDate,cluster_no))
  #return []


@app.route('/getOneDayClusterData/<endDate>/<int:cluster_no>')
@cross_origin()
def getOneDayClusterData(endDate,cluster_no):
  try:
    startDate=str(date(int(endDate[0:4]),int(endDate[5:7]),int(endDate[8:10]))-timedelta(days=1))
    newDictList = getClusterDataList(startDate,endDate,cluster_no)
    if(newDictList[0]['date']==str(endDate) or newDictList[-1]['date']==str(endDate)):
      clusterData={}
      clusterData['date']=endDate
      clusterData["rank"]=int(newDictList[-1]['rank'])
      clusterData["size"]=newDictList[-1]['size']
      clusterData["keywords"]=newDictList[-1]["keywords"]
      clusterData['rankChange']=clusterData["rank"]
      clusterData['sizeChange']=clusterData["size"]
      if(newDictList[0]['date']==str(startDate)):
        clusterData["rankChange"]-=int(int(newDictList[0]['rank']))
        clusterData["sizeChange"]-=int(newDictList[0]['size'])
      return json.dumps([clusterData])
    else:
      return json.dumps([])
  except Exception as e:
    print("Error with one date Cluster Data api ",e)
    return json.dumps([])
   
 

