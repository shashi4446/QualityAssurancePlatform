from pymongo import MongoClient
from bson.json_util import dumps
class RegionData:
    """
    The stationdata collection stores the regions and the stations information in each region
    The data is represented as:
    {"region": regionname, "stations": [{"station": stationname, "code':code, "lat": lattitude, "lng": longitude}{...}{..}]}
    """

    def __init__(self):
        pass

    def getAllRegionInfo(self):
        """
        This function fetches all the records from the collection "stationdata"
        :return: all the region records
        """
        regions = []
        client = MongoClient()  # setting connection with the mongoclient
        db = client.qaplatformdb  # getting database
        collection = db.stationdata #getting stationdata collection
        data = collection.find()
        if data.count() != 0:
            for item in data:
                regions.append(item)
            return regions
        else:
            return None

    def getSingleRegionInfo(self, region):
        """
        This function reads the record for one region from the collection "stationdata"
        :return:
        """

        client = MongoClient()  # setting connection with the mongoclient
        db = client.qaplatformdb  # getting database
        collection = db.stationdata #getting stationdata collection
        data = collection.find_one({"region":region})
        if data.count()!= 0:
            return {'data':data}
        else:
            return None

    def getSingleStationInfo(self,region, station_name):
        """
        This function fetches single records from the collection "stationdata" whose name matches to the input station
        :return: single record for the station in the region
        """
        client = MongoClient()  # setting connection with the mongoclient
        db = client.qaplatformdb  # getting database
        collection = db.stationdata #getting stationdata collection
        result = collection.find_one({"region": region})
        if result.count() != 0:
            stations = result['stations']
            for station in stations:
                if (station["station"] == station_name):
                    return station
        else:
            return None

    def getStaionCode(self, region, station):
        """
        :param region:
        :param station:
        :return: code of the input station
        """
        client = MongoClient()  # setting connection with the mongoclient
        db = client.qaplatformdb  # getting database
        collection = db.stationdata  # getting stationdata collection
        result = collection.find_one({"region": region})
        if result.count() != 0:
            stations = result["stations"]
            for s in stations:
                if s["station"]== station:
                    code = s["code"]
                    return code
        else :
            return None


    def insertRegionInfoIntoDB(self, post_data):
        """
        database name: "qaplatformdb"
        collection name: "stationdata"
        This method is used to insert stations information into the collection
        The information format is
        {_id: regionId,
        region: regionname,
        stations: [{staion: staitonname, code: stationcode, lat: lattitude, lng: longitude},.....]
        }
        :return:
        """

        client = MongoClient()  # setting connection with the mongoclient
        db = client.qaplatformdb  # getting database
        collection = db.stationdata #getting stationdata collection
        # inserting document to mongodb
        result = collection.insert_one(post_data)


# --------------------------------- Querying the validated data sets ------------------------------------------------
class ValidatedData:
    """
    The validateddata collection consists of validated data for each station
    """
    def __init__(self):
        pass

    def insertValidatedStationData(self, region, station, start_date, end_date,totalquality, data, parameters):
        """
        This function inserts the validated dataset into the database
        { region: 'regionname' , station: 'stationname',
         from: 'startdate', to: 'enddate',
        data: "data", TotalWaterQuality: "totalquality"
        qualityParameters: {completeness: '', accuracy:'',timeliness:'',uniqueness:'',validity:'',consistency:'',
         reliability:'', usability:''}
        }
        :param region:
        :param station:
        :param start_date:
        :param end_date:
        :return:
        """
        client = MongoClient()  # setting connection with the mongoclient
        db = client.qaplatformdb  # getting database
        collection = db.validateddata #getting validateddata collections

        post_data = {'region':region, 'station': station, 'start_date':start_date,'end_date':end_date,
                     'TotalWaterQuality':totalquality, 'qualityparameters':parameters,'data': data}
        result = collection.insert_one(post_data)

    def searchAllValidatedDataForRegion(self,region):
        """
                This function searches for all the records for the given region in the validated database
                :param region:
                :param station:
                :return: returns the region record without data field
                """
        records = []  # list to store all the records of the stations in a region
        client = MongoClient()  # setting connection with the mongoclient
        db = client.qaplatformdb  # getting database
        collection = db.validateddata  # getting validateddata collections
        result = collection.find({"region": region}, {"data": 0})
        if result.count() != 0:
            for record in result:
                records.append(record)
            return records
        else:
            return None

    def searchAllValidatedDataForStation(self, region, station):
        """
        This function reads all station data for all the dates from validateddata collection
        :param region:
        :param station:
        :return: returns the station record without data field
        """
        records =[] # list to store all the records of the stations in a region
        client = MongoClient()  # setting connection with the mongoclient
        db = client.qaplatformdb  # getting database
        collection = db.validateddata  # getting validateddata collections
        result = collection.find({"region": region, "station": station},{"data":0})

        if result.count()!= 0:
            for record in result:
                records.append(record)
            return records
        else:
            return None

    def searchValidatedDataForStation(self, region, station, start_date, end_date):
        """
        This function searches the record of the station between start and end date into collection of validated dataset
        validatedcollection stores data in following format
        { region: 'regionname' , station: 'stationname', from: 'startdate', to: 'enddate', type: "water quality",
        'TotalWaterQuality':totalquality, data: "data",
        qualityparameters: {completeness: '', accuracy:'',timeliness:'',uniqueness:'',validity:'',consistency:'', reliability:'', usability:''}
        }
        :param region:
        :param station:
        :param start_date:
        :param end_date:
        :return: returns record without data field if found, else return empty body
        """
        records = []
        client = MongoClient()  # setting connection with the mongoclient
        db = client.qaplatformdb  # getting database
        collection = db.validateddata #getting validateddata collection
        result = collection.find({'$and': [{"region": region}, {"station":station}, {"start_date":start_date}, {"end_date": end_date}]},{"data":0})
        if result.count() != 0:
            records.append(result)
            return records
        else:
            return None


# --------------------------------- Querying the raw data sets ------------------------------------------------

class RawData:
    """
    The rawdata collection stores the raw data obtained from the webservices
    """
    def __init__(self):
        pass

    def insertRawStationData(self, region, station, start_date, end_date, data):
        """
        This function inserts the validated dataset into the database
        { region: 'regionname' , station: 'stationname', from: 'startdate', to: 'enddate', data: "data",
         }
        :param region:
        :param station:
        :param start_date:
        :param end_date:
        :return:
        """
        client = MongoClient()  # setting connection with the mongoclient
        db = client.qaplatformdb  # getting database
        collection = db.rawdata # getting rawdata collection

        post_data = {'region': region, 'station': station, 'start_date': start_date, 'end_date': end_date,'data': data}
        result = collection.insert_one(post_data)

    def getRawData(self,region, station, start_date, end_date):
        """
        This function reads station data from raw database
        :param region:
        :param station:
        :param start_date:
        :param end_date:
        :return: returns the station record
        """
        client = MongoClient()  # setting connection with the mongoclient
        db = client.qaplatformdb  # getting database
        collection = db.rawdata  # getting rawdata collection
        result = collection.find({'$and': [{"region": region}, {"station": station}, {"start_date": start_date}, {"end_date": end_date}]})
        if result.count() !=0:
            return result
        else:
            return None

