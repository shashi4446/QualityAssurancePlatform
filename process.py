import DataCleaning
from calculateQP import QualityParameterCalculation
from calculateTotalWaterQuality import WaterQuality

class DataProcessing:
    def process(self, region, station, start_date, end_date, source, data, clean, modelBasedValidationType, model, parameters):
        info = { 'start_date': start_date, 'end_date': end_date, 'region': region, 'station': []}

        if modelBasedValidationType == "stationbased":
            {}

        elif modelBasedValidationType == "regionbased" :
            dcObj =  DataCleaning()
            dcObj.cleanData(region, station, start_date, end_date)

        qpObj = QualityParameterCalculation()
        resultParameters = qpObj.calculateParameters(parameters)

        oqObj = WaterQuality()
        overallQuality = oqObj.getTotalWaterQuality(model)
        pass