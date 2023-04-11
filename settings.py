## Settings file
from calendar import month
import datetime
import time


## This module runs the current time and HE. This is the module that will decide when certain modules should be run,
## where data pipelines should exit, and where concurrency needs real time vs historical data. To keeps things running fast, 
## data will be generalized in the immediate past (outside flowing hour), exact and literal in the present flowing hours, and updating
## very fast on the same day. The 1 day out portions will not be updating as fast, except for the availability modules.

## Will try to keep things as strings or ints to keep it simple. Anything that is pushed to a different module will use pandas 
## library to convert back to a datetime format. 
PASSWORD = 'anisoTropical308'
USERNAME = 'micoconnell@suncor.com'
TwentyFourMonthAPI = 278763
enmax_API = 245961
weather_API = 5
load_API = 225
futureLoad_API = 17
smp_API = 1
interchange_API = 224
### Generation API call numbers
coal_API = 86
energyStorage_API = 322677
dualFuel_API = 322684
hydro_API  = 87
gas_API = 85
windNET_API = 23694
solarNET_API = 322665

coal_DCR = 80
hydro_DCR = 79
gas_DCR = 78
energy_DCR = 322680
solar_DCR = 322669
wind_DCR = 293674
dualFuel_DCR = 322687
other_DCR = 293675
DCR_DCR = 4120
DCR_Current = 228

historicalAvab_CoalAPI = 118369
historicalAvab_HydroAPI = 118371
historicalAvab_GasAPI = 118370
historicalAvab_SolarAPI = 322668
historicalAvab_WindAPI = 147264
historicalAvab_OtherAPI = 118372
historicalAvab_EnergyAPI = 322679

futureDCRActiveSpinning = 6989
futureDRCActiveSupp = 6990



def todays_date():
    csvline= str(datetime.datetime.now().date())
    DATESTART = csvline
    return DATESTART

def formatted7day_date():
    csvline= str(datetime.datetime.now().date() + datetime.timedelta(days=7))
    DATESTART = csvline
    #csvlineDelta=DATESTART + datetime.timedelta(days=7)
    #csvlineDelta = str(csvlineDelta)
    return DATESTART


def username_Password(USERNAME,PASSWORD):
    USERNAME = USERNAME
    PASSWORD = PASSWORD

def currency_SETTER():
    dt = str(datetime.datetime.now().replace(second=0, microsecond=0))
    currentHE = int(time.strftime("%H"))
    return dt,currentHE
def hour_Designator(currencySetter):
    result = currency_SETTER()
    result = result[1]
    result += 1
    if result == 25:
        result = 24
    return result

## This method will grab the T-2 hours.
def next_FlowingHours(hour_Designator):
    flowingHours = hour_Designator
    flowingHour1 = flowingHours + 1
    if flowingHour1 == 25:
        flowingHour1 = 1
        boolVal = 1
    flowingHour2 = flowingHour1 + 1
    return flowingHours,flowingHour1,flowingHour2

## The next two methods determine when exactly the time will be the API should switch over from the 12 hour forecast to the 7 day forecast. Remember, the 12 hour
## forecast is updated every 10 minutes. The 7 day is updated every 1 hour.
def twelveHour_Forecast():
    result = currency_SETTER()
    result = result[1]
    result = result+13
    if result == 24:
        boolVal = 0
        return result,boolVal
    elif result >= 25:
        result = result - 24
        boolVal = 1
        return result,boolVal
def twelveHour_ForecastDATE():
    result = twelveHour_Forecast()
    if result[1] > 0:
        dt = datetime.datetime.now().replace(hour=0,minute=0,second=0, microsecond=0)
        dt = dt + datetime.timedelta(days=1)
        dt = dt + datetime.timedelta(hours=result[0])
        return str(dt)
    elif result[1] == 0:
        dt = datetime.datetime.now().replace(hour=0,minute=0,second=0, microsecond=0)
        dt = dt + datetime.timedelta(hours=result[0])
        return str(dt)

    
def prev7Days_Date():
    csvlineDelta = datetime.datetime.now().date()
    csvlineDelta=csvlineDelta + datetime.timedelta(days=-7)
    csvlineDelta = str(csvlineDelta)
    csvlineDelta = csvlineDelta.replace("-", "/")
    return csvlineDelta
def forward14Days_Date():
    csvlineDelta = datetime.datetime.now().date()
    csvlineDelta=csvlineDelta + datetime.timedelta(days=14)
    csvlineDelta = str(csvlineDelta)
    csvlineDelta = csvlineDelta.replace("-", "/")
    return csvlineDelta

def current_Date():
    csvlineDelta = datetime.datetime.now().date()
    csvlineDelta = str(csvlineDelta)
    csvlineDelta = csvlineDelta.replace("-", "/")
    return csvlineDelta






def past7_DateAESOFORMAT():
    csvlineDelta = datetime.datetime.now().date()
    csvlineDelta=csvlineDelta + datetime.timedelta(days= -7)
    csvlineDelta = str(csvlineDelta)
    csvlineDelta = csvlineDelta.replace("-", "")
    return csvlineDelta    
def forward7_DateAESOFORMAT():
    csvlineDelta = datetime.datetime.now().date()
    csvlineDelta=csvlineDelta + datetime.timedelta(days= 7)
    csvlineDelta = str(csvlineDelta)
    csvlineDelta = csvlineDelta.replace("-", "")
    return csvlineDelta    
def forward14_DateAESOFORMAT():
    csvlineDelta = datetime.datetime.now().date()
    csvlineDelta=csvlineDelta + datetime.timedelta(days= 14)
    csvlineDelta = str(csvlineDelta)
    csvlineDelta = csvlineDelta.replace("-", "")
    return csvlineDelta 

def forward14Days_Date():
    csvlineDelta = datetime.datetime.now().date()
    csvlineDelta=csvlineDelta + datetime.timedelta(days=14)
    csvlineDelta = str(csvlineDelta)
    csvlineDelta = csvlineDelta.replace("-", "/")
    return csvlineDelta

def current_DateAESOFORMAT():
    csvlineDelta = datetime.datetime.now().date()
    csvlineDelta = str(csvlineDelta)
    csvlineDelta = csvlineDelta.replace("-", "")
    return csvlineDelta

def todayDays_Date():
    csvlineDelta = datetime.datetime.now().date()
    csvlineDelta=csvlineDelta + datetime.timedelta(days= 1)
    csvlineDelta = str(csvlineDelta)
    csvlineDelta = csvlineDelta.replace("-", "/")
    return csvlineDelta    
## This next section will determine     
    
    
try:
    relevant_FlowingHours = next_FlowingHours(hour_Designator(currency_SETTER))
    print(prev7Days_Date())
except zeroDivError:
    print("")

