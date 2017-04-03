import os

import datetime
import uuid

from flask import jsonify, request, session, json, redirect, flash
from flask import Flask, render_template, app
from model import RegionData, ValidatedData, RawData
from process import DataProcessing
from werkzeug.utils import secure_filename


UPLOAD_FOLDER = './data/csv'
ALLOWED_EXTENSIONS = set(['csv', 'xls','txt'])

app = Flask(__name__)  # define app using Flask
app.secret_key = '445424ad-df86-4c03-91ce-cc4b0d5dc9d3'  # Secret key
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

@app.route('/')
def gethome():
    return render_template('index.html')


@app.route('/index')
def getindex():
    return render_template('index.html')


# api to return all the regions and station information
@app.route('/getallregioninfo', methods=['GET'])
def getRegionInformation():
    obj = RegionData()
    data = obj.getAllRegionInfo()

    # return data
    return jsonify({"result": json.loads(dumps(data))})


# api to search the station record in the validated dataset
@app.route('/search', methods=['POST'])
def searchData():
    """
    searches the record for the station into database
    :return: if data is found into the database
                return { found: "yes", data: { record}}
            else
                return { found: "no", data: {}}
    """

    # extract the parameters from the request body
    input_json = request.get_json(force=True)
    region = request.json["region"]  # region name
    station = request.json["station"]  # station name
    if len(station) is 0:
        station = None
    start_date = request.json["from"]  # start_date name
    if len(start_date) is 0:
        start_date = None
    end_date = request.json["to"]  # end_date name
    if len(end_date) is 0:
        end_date = None

    # query the database
    obj = ValidatedData()
    if (region != None and station != None and start_date != None and end_date != None):
        records = obj.searchValidatedDataForStation(region, station, start_date, end_date)
    elif (region != None and station != None):
        records = obj.searchAllValidatedDataForStation(region, station)
    elif (region != None):
        records = obj.searchAllValidatedDataForRegion(region)

    if (records is not None):
        return jsonify(data={'found': 'yes', 'records': dumps(records)})
    else:
        return jsonify(data={'found': "no"})


"""
#api to collect user input file
@app.route('/getUserFile', methods=['POST'])
def getUserDataFile():
    input_json = request.get_json(force=True)

    region = request.json["region"]  # region name
    print "region: " + region

    station = request.json["station"]  # station name
    print "station: " + station

    start_date = request.json["from"]  # start_date name
    print "from: " + start_date

    end_date = request.json["to"]  # end_date name
    print "to: " + end_date
    #code for uploading file


#api to collect user input filters to collect data from webservice
@app.route('/getDataFromWebService', methods=['POST'])
def getDataFromAPI():
    input_json = request.get_json(force=True)

    region = request.json["region"]  # region name
    print "region: " + region

    station = request.json["station"]  # station name
    print "station: " + station

    start_date = request.json["from"]  # start_date name
    print "from: " + start_date

    end_date = request.json["to"]  # end_date name
    print "to: " + end_date
    #code for downloading data

"""


# api to calculate the quality parameters
@app.route('/getWaterQuality', methods=['POST'])
def getWaterQuality():
    """
    this api collects the user input and then process the data to calculate the quality paramaters
    :return: result = {"region": regionname , "station": stationname,
         "from": startdate, "to": enddate, "TotalWaterQuality": totalquality,
        "qualityParameters": {"completeness": cp, "accuracy": a, "timeliness": t, "uniqueness": un, "validity" : v, "consistency": c,
         reliability: r, "usability"": us} }
    """
    # return "data received"

    input_json = request.get_json(force=True)
    print(input_json)
    region = input_json['Region']  # region name

    station = input_json['Station']  # station name

    start_date = input_json['FromDate']  # start_date name

    end_date = input_json['ToDate']  # end_date name

    clean = input_json["IsRequiredClean"]  #

    source =input_json["Source"]

    getdatafromWebAPI = True
    if source =="CSV" :
        csvfilename = input_json["CsvFileName"]
        getdatafromWebAPI = False



    parameters = {'isCompleteness': input_json["Parameters"]["Completeness"],
                  'isAccuracy': input_json["Parameters"]["Accuracy"],
                  'isTimeliness': input_json["Parameters"]["Timeliness"],
                  'isUniqueness': input_json["Parameters"]["Uniqueness"],
                  'isValidity': input_json["Parameters"]["Validity"],
                  'isConsistency': input_json["Parameters"]["Consistency"],
                  'isReliability': input_json["Parameters"]["Reliability"],
                  'isAvailability': input_json["Parameters"]["Availability"],
                  'isUsability': input_json["Parameters"]["Usability"],
                  };

    model = request.json["Model"]  #
    data = ""
    modelBasedValidationType =""

    validationType = input_json["ValidationType"]
    if (validationType == "Toolbased"):
        pass
    if (validationType == "Modelbased"):
        modelBasedValidationType = input_json["ModelBasedSubType"]


    if getdatafromWebAPI == True:
        source = "webapi"
        data = ""

    processingObj = DataProcessing()
    answer = processingObj.process(region, station, start_date, end_date, source, data, clean, modelBasedValidationType,
                                   model, parameters)

    result = {}
    return jsonify(data=result)


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/upload', methods=['GET', 'POST'])
def upload():
    if request.method == 'POST':
        # check if the post request has the file part
        if 'csvfile' not in request.files:
            flash('No file part')
            return jsonify( {"result": {"status": "failed - No File Part"}})
            #  return redirect(request.url)
        file = request.files['csvfile']
        # if user does not select file, browser also
        # submit a empty part without filename
        if file.filename == '':
            flash('No selected file')
            return jsonify({"result": 'no file selected'})
        if file and allowed_file(file.filename):
            filename = str(uuid.uuid4()) +"_" + secure_filename(file.filename)
            #  saveatlocation = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            saveatlocation = app.config['UPLOAD_FOLDER']
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            return jsonify(result ={"filename": filename})


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)  # run app in debug mode
