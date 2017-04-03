from bsddb import dbutils

import SOAPpy
import simplejson
import json
import re
from model import RegionData, RawData, ValidatedData

class DataCollectionFromWebService:
    """
    This class contains methods to collect data from the SOAP webservice, store the data into a json file, store the data to rawdata collection
    """
    filename =''
    def __init__(self):
        pass

    def getDatafromWebService(self,region, station, start_date, end_date):
        """
        This function is used to collect the data from National Oceanic and Atmospheric Administration
        :param stationcode:
        :param startdate:
        :param enddate:
        :return:
        """
        #construct filename in the format "region_station_startdate_enddate.json" with no spaces and "-"

        filename = region + "_" + station+ "_" + start_date + "_" + end_date + ".json"
        filename = filename.replace(" ","")
        filename = filename.replace("-","")
        print filename
        obj =  RegionData()
        stationcode = obj.getStaionCode(region, station)


        server = SOAPpy.SOAPProxy("http://cdmo.baruch.sc.edu/webservices2/requests.cfc?wsdl")

        stationcode="pdbjewq"
        responsedata =  server.exportAllParamsDateRangeXMLNew(stationcode, start_date, end_date,'*')
       # print responsedata
        pythonObject = SOAPpy.Types.simplify(responsedata)
        #jsonObject = json.dumps(pythonObject)
        #assert type(jsonObject) == str
        dataArray =  pythonObject["returnData"]["data"] # returns {  [{...},{....},.....]}
        #print dataArray

        self.dataToJson(dataArray, filename) # store the data into a json file
        #store data into rawdata collection

        rawObj =RawData()
        rawObj.insertRawStationData(region,station,start_date,end_date,dataArray)
        return filename # return the json filename where data is stored

    def dataToJson(self,dataArray, filename):
        """
        This function stores the input data into a json file and stores it in ./data folder
        :param dataArray:
        :return: json file name
        """
        try:
            jsondata = simplejson.dumps(dataArray, indent=4, skipkeys=True, sort_keys=True)
            fd = open("./data/" + filename, 'w')
            fd.write(jsondata)
            fd.close()
            print ("data written to the file succesfully")
        except:
            print 'ERROR writing', filename
        pass





