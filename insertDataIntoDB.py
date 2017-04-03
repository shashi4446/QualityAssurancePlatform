from pymongo import MongoClient
import json
from model import RegionData
import SOAPpy
"""
data = {'region':"Padilla Bay, WA", 'stations':[
                    {'station':"Bayview Channel", 'code': "pdbbywq", 'lat':"48.496139",'lng':"122.502114"},
                    {'station':"Ploeg Channel", 'code': "pdbbpwq", 'lat':"48.556322",'lng':"122.530894"},
                    {'station':"Joe Leary Estuary", 'code': "pdbjewq", 'lat':"48.518264",'lng':"122.474189"},
                    ]}

obj = RegionData()
"""
server = SOAPpy.SOAPProxy("http://cdmo.baruch.sc.edu/webservices2/requests.cfc?wsdl")
responsedata =  server.exportAllParamsDateRangeXMLNew('pdbjewq', '2014-12-30', '2014-12-31','*')

pythonObject = SOAPpy.Types.simplify(responsedata)
#print responsedata
dataArray =  pythonObject["returnData"]["data"]
print dataArray

""" code to import data from file to mongodb0























with open('ElkhornSlough,CA_SouthMarch_20141230_20141231.json') as data_file:
    data = json.load(data_file)
client = MongoClient()  # setting connection with the mongoclient
db = client.test  # getting database
collection = db.test #getting validateddata collection
db.test.insert(data)
print db.test.find().pretty()
"""