import http.client
import json
import time
import csv
from datetime import datetime
from datetime import timedelta
import re
from tracemalloc import start
import pandas as pd
import os
import zipfile
import certifi
import ssl
import sys
sys.path.append("../..")
import settings
import shutil


pd.options.display.max_rows = 999



def renewables_ScrapeForecast(switchNumber,startDateDASHED,endDateDASHED):
    startDateDASHED = startDateDASHED
    endDateDASHED = endDateDASHED
    switchNumber = switchNumber
    def aeso_12HourWind(startDateDASHED,endDateDASHED): 
        url="http://ets.aeso.ca/Market/Reports/Manual/Operations/prodweb_reports/wind_solar_forecast/wind_rpt_shortterm.csv"
        c=pd.read_csv(url)
        c['Forecast Transaction Date'] = pd.to_datetime(c['Forecast Transaction Date'])
        c = c.set_index('Forecast Transaction Date')
        c.sort_index(inplace=True, ascending=True)
        c = c.loc[startDateDASHED : endDateDASHED]
        return c
    def aeso_12HourSolar(startDateDASHED,endDateDASHED): 
        url="http://ets.aeso.ca/Market/Reports/Manual/Operations/prodweb_reports/wind_solar_forecast/solar_rpt_shortterm.csv"
        c=pd.read_csv(url)
        c['Forecast Transaction Date'] = pd.to_datetime(c['Forecast Transaction Date'])
        c = c.set_index('Forecast Transaction Date')
        c.sort_index(inplace=True, ascending=True)
        c = c.loc[startDateDASHED : endDateDASHED]
        return c

    def aeso_7DayWind(startDateDASHED,endDateDASHED): 
        url="http://ets.aeso.ca/Market/Reports/Manual/Operations/prodweb_reports/wind_solar_forecast/wind_rpt_longterm.csv"
        c=pd.read_csv(url)
        c['Forecast Transaction Date'] = pd.to_datetime(c['Forecast Transaction Date'])
        c=c.set_index('Forecast Transaction Date')
        c.sort_index(inplace=True, ascending=True)
        c = c.loc[startDateDASHED : endDateDASHED]
        return c

    def aeso_7DaySolar(startDateDASHED,endDateDASHED): 
        url="http://ets.aeso.ca/Market/Reports/Manual/Operations/prodweb_reports/wind_solar_forecast/solar_rpt_longterm.csv"
        c=pd.read_csv(url)
        c['Forecast Transaction Date'] = pd.to_datetime(c['Forecast Transaction Date'])
        c=c.set_index('Forecast Transaction Date')
        c.sort_index(inplace=True, ascending=True)
        c = c.loc[startDateDASHED : endDateDASHED]
        return c


    switcher = {
    1: aeso_12HourWind,
    2: aeso_12HourSolar,
    3: aeso_7DayWind,
    4: aeso_7DaySolar,
    }
    
    def switch(switchNumber):
        return switcher.get(switchNumber)(startDateDASHED,endDateDASHED)
    

    return switch(switchNumber)



dfshortWind = pd.DataFrame()
dfshortSolar = pd.DataFrame()
dflongWind = pd.DataFrame()
dflongSolar = pd.DataFrame()
dfshortWind = renewables_ScrapeForecast(1,settings.current_DateAESOFORMAT(),settings.current_DateAESOFORMAT())
dfshortSolar = renewables_ScrapeForecast(2,settings.current_DateAESOFORMAT(),settings.current_DateAESOFORMAT())

dflongWind = renewables_ScrapeForecast(3,settings.current_DateAESOFORMAT(),settings.forward7_DateAESOFORMAT())
dflongSolar = renewables_ScrapeForecast(4,settings.current_DateAESOFORMAT(),settings.forward7_DateAESOFORMAT())


