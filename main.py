

#Settings will control the timers to get the correct date and time for the modules
import timeit
from datetime import tzinfo
import settings
import pandas as pd
import pytz
import datetime as dt
import schedule
import time
#Import the currents used to build the pipeline.
starttime = timeit.default_timer()

## Create dynamic timestamp for data truncation
import datetime as dt
current = dt.datetime.now()
current_td = dt.timedelta(
    hours = current.hour, 
    minutes = current.minute, 
    seconds = current.second, 
    microseconds = current.microsecond)
to_hour = dt.timedelta(hours = round(current_td.total_seconds() // 3600))
tohour = dt.datetime.combine(current, dt.time(0)) + to_hour
print(tohour)
#################################################################################################
import Current
import CurrentPlusOne
import Historic
import Future

def past():
    pastDF = Historic.Historicals()
    return pastDF
def pastAlgo(pastHistorical):
    pastHistorical = pastHistorical
    revampedHistorcial = pastHistorical
    revampedHistorcial.rename(columns = {"Coal Availability": "coal_Avab","Hydro":"hydro_Avab","Energy":"energy_Avab","Gas":"gas_Avab","Hydro":"hydro_Avab"},inplace = True)
    revampedHistorcial.rename(columns = {"Other": "other_Avab","Total Historical Generation Available":"TotalHistoricalGen","Delta Interchange":"Delta_Interchange","Historical DCR":"DCR"},inplace = True)
    revampedHistorcial.rename(columns = {"Wind": "wind_Avab","Solar":"solar_Avab","Final Supply Cushion":"supply_Cushion"},inplace = True)
    revampedHistorcial.to_csv('sad.csv')
    return revampedHistorcial






def currently():
    currentDF = Current.currents()
    currentDF = currentDF.drop(columns = ['Date/Time'])
    currentDF.rename(columns = {"Avab_Coal": "coal_Avab","Avab_Hydro":"hydro_Avab","Avab_Energy":"energy_Avab","Avab_Gas":"gas_Avab","Avab_Other":"Avab_Other"},inplace = True)
    currentDF.rename(columns = {"Solar": "Gen_Solar","Wind":"Gen_wind","Interchange_Flowing":"Delta_Interchange"},inplace = True)
    currentDF.rename(columns = {"FinalCurrentSupply": "FinalHistoricalSupply","FinalCurrent":"supply_Cushion"},inplace = True)
    print(currentDF)
    return currentDF

def currentone():
    currentPlusDF = CurrentPlusOne.currentplusone()
    currentPlusDF.rename(columns = {"Avab_Coal": "coal_Avab","Avab_Hydro":"hydro_Avab","Avab_Energy":"energy_Avab","Avab_Gas":"gas_Avab","Avab_Other":"Avab_Other"},inplace = True)
    currentPlusDF.rename(columns = {"Solar": "Gen_Solar","Wind":"Gen_wind","Interchange_Flowing":"Delta_Interchange"},inplace = True)
    currentPlusDF.rename(columns = {"FinalCurrentSupply": "FinalHistoricalSupply","FinalCurrent":"supply_Cushion"},inplace = True)
    print(currentPlusDF)
    return currentPlusDF

def future():
    FutureFinal = Future.futures()
    return FutureFinal


pastHistorical = past()
HistoricalFormatCorrected = pastAlgo(pastHistorical)

CurrentHour = currently()



NextHour = currentone()
futureDF = future()
################################################################


supplycushionDF = HistoricalFormatCorrected['supply_Cushion']
supplycushionDF = supplycushionDF.append(CurrentHour['supply_Cushion'])
supplycushionDF = supplycushionDF.append(NextHour['supply_Cushion'])
supplycushionDF = supplycushionDF.append(futureDF['supply_Cushion'])
print(supplycushionDF)
supplycushionDF = supplycushionDF.to_csv()

print(timeit.default_timer() - starttime)
schedule.every(10).minutes.do(past)
schedule.every(1).minutes.do(currently)
schedule.every(2).minutes.do(currentone)
schedule.every(3).minutes.do(future)
#schedule.every().hour.do(Historic.Historicals())
# from azure.storage.blob import BlobServiceClient
# blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=sevendaypremium;AccountKey=YeFdLE5sLLsVceijHjRczp3GgZ70AtN4pHmTDlL73a98Om5SmWVL3WIA9xWo4hQ84u3FCirCqM3P+AStlvSSrQ==;EndpointSuffix=core.windows.net")
# container_client = blob_service_client.get_container_client("checktext")
# blob_client = container_client.get_blob_client("df.csv")
# container_client = blob_client.upload_blob(supplycushionDF,overwrite=True)
# while True:

# 	# Checks whether a scheduled task
# 	# is pending to run or not
# 	schedule.run_pending()
