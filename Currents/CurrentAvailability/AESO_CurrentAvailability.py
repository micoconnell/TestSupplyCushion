import datetime
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import requests
import json
from functools import reduce
from datetime import datetime  
import sys
sys.path.append("../..")
import settings
import shutil





def sevenday_Availability(switchNumber,startDateDASHED,endDateDASHED):    
    url = 'http://ets.aeso.ca/ets_web/ip/Market/Reports/SevenDaysHourlyAvailableCapabilityReportServlet?contentType=html'
    listOFBULLSHIT = ['1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24']
    tables = pd.read_html(url)
    df = tables[2:5][0]
    df.columns = df.iloc[0]
    df=df.drop(df.index[0], axis = 0)
    newdf = df.dropna(axis='columns')
    newdf.columns = ['AssetType','Date','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24']
    df = newdf[newdf["AssetType"].str.contains("&nbsp") == False]
    df['Date'] = pd.to_datetime(df['Date'])
    df[['AssetType','discard1']] = df['AssetType'].str.split('(',expand=True)
    df[['discard1','discard2']] = df['discard1'].str.split(')',expand=True)
    df[['discard1','discard3']] = df['discard1'].str.split('=',expand=True)
    df[['discard3','discard4']] = df['discard3'].str.split('M',expand=True)
    df = df.drop('discard1',axis=1)
    df = df.drop('discard2',axis=1)
    df = df.drop('discard4',axis=1)
    for x in listOFBULLSHIT:
        df[x] = df[x].str.rstrip('%').astype('float') / 100.0
        df['discard3'] = df['discard3'].astype('float')
    for x in listOFBULLSHIT:
        df[x] = df[x] * df.discard3.values
    df = df.drop('discard3',axis=1)
    paraGroup = df.groupby('AssetType')
    

    startDateDASHED = startDateDASHED
    endDateDASHED = endDateDASHED
    switchNumber = switchNumber
    def coal(paraGroup,startDateDASHED,endDateDASHED):
        paraGroup = paraGroup.get_group('COAL')
        paraGroup = paraGroup.set_index('Date')
        paraGroup.sort_index(inplace=True, ascending=True)
        paraGroup = paraGroup.loc[startDateDASHED : endDateDASHED]
        df = paraGroup
        df = df.drop('AssetType',axis=1)
        cols = np.arange(len(df.columns))
        df.columns = [ cols // 24, cols % 24]
        cols = np.arange(len(df.columns))
        df.columns = [ cols // 24, cols % 24]
        df1 = df.stack().rename_axis((None, None)).reset_index(level=1, drop=True).reset_index()
        df1=df1.set_index('index')
        df1.columns = ['Actuals']
        return df1
    def hydro(paraGroup,startDateDASHED,endDateDASHED): 
        paraGroup = paraGroup.get_group('HYDRO')
        paraGroup = paraGroup.set_index('Date')
        paraGroup.sort_index(inplace=True, ascending=True)
        paraGroup = paraGroup.loc[startDateDASHED : endDateDASHED]
        df = paraGroup
        df = df.drop('AssetType',axis=1)
        cols = np.arange(len(df.columns))
        df.columns = [ cols // 24, cols % 24]
        cols = np.arange(len(df.columns))
        df.columns = [ cols // 24, cols % 24]
        df1 = df.stack().rename_axis((None, None)).reset_index(level=1, drop=True).reset_index()
        df1=df1.set_index('index')
        df1.columns = ['Actuals']
        return df1
    def gas(paraGroup,startDateDASHED,endDateDASHED): 
        paraGroup = paraGroup.get_group('GAS')
        paraGroup = paraGroup.set_index('Date')
        paraGroup.sort_index(inplace=True, ascending=True)
        paraGroup = paraGroup.loc[startDateDASHED : endDateDASHED]
        df = paraGroup
        df = df.drop('AssetType',axis=1)
        cols = np.arange(len(df.columns))
        df.columns = [ cols // 24, cols % 24]
        cols = np.arange(len(df.columns))
        df.columns = [ cols // 24, cols % 24]
        df1 = df.stack().rename_axis((None, None)).reset_index(level=1, drop=True).reset_index()
        df1=df1.set_index('index')
        df1.columns = ['Actuals']
        return df1
    def biofuel(paraGroup,startDateDASHED,endDateDASHED): 
        paraGroup = paraGroup.get_group('BIOMASS and OTHER')
        paraGroup = paraGroup.set_index('Date')
        paraGroup.sort_index(inplace=True, ascending=True)
        paraGroup = paraGroup.loc[startDateDASHED : endDateDASHED]
        df = paraGroup
        df = df.drop('AssetType',axis=1)
        cols = np.arange(len(df.columns))
        df.columns = [ cols // 24, cols % 24]
        cols = np.arange(len(df.columns))
        df.columns = [ cols // 24, cols % 24]
        df1 = df.stack().rename_axis((None, None)).reset_index(level=1, drop=True).reset_index()
        df1=df1.set_index('index')
        df1.columns = ['Actuals']
        return df1
    def energy(paraGroup,startDateDASHED,endDateDASHED): 
        paraGroup = paraGroup.get_group('ENERGY STORAGE')
        paraGroup = paraGroup.set_index('Date')
        paraGroup.sort_index(inplace=True, ascending=True)
        paraGroup = paraGroup.loc[startDateDASHED : endDateDASHED]
        df = paraGroup
        df = df.drop('AssetType',axis=1)
        cols = np.arange(len(df.columns))
        df.columns = [ cols // 24, cols % 24]
        cols = np.arange(len(df.columns))
        df.columns = [ cols // 24, cols % 24]
        df1 = df.stack().rename_axis((None, None)).reset_index(level=1, drop=True).reset_index()
        df1=df1.set_index('index')
        df1.columns = ['Actuals']
        return df1
    def wind(paraGroup,startDateDASHED,endDateDASHED): 
        paraGroup = paraGroup.get_group('WIND')
        paraGroup = paraGroup.set_index('Date')
        paraGroup.sort_index(inplace=True, ascending=True)
        paraGroup = paraGroup.loc[startDateDASHED : endDateDASHED]
        df = paraGroup
        df = df.drop('AssetType',axis=1)
        cols = np.arange(len(df.columns))
        df.columns = [ cols // 24, cols % 24]
        cols = np.arange(len(df.columns))
        df.columns = [ cols // 24, cols % 24]
        df1 = df.stack().rename_axis((None, None)).reset_index(level=1, drop=True).reset_index()
        df1=df1.set_index('index')
        df1.columns = ['Actuals']
        return df1
    def solar(paraGroup,startDateDASHED,endDateDASHED): 
        paraGroup = paraGroup.get_group('SOLAR')
        paraGroup = paraGroup.set_index('Date')
        paraGroup.sort_index(inplace=True, ascending=True)
        paraGroup = paraGroup.loc[startDateDASHED : endDateDASHED]
        df = paraGroup
        df = df.drop('AssetType',axis=1)
        cols = np.arange(len(df.columns))
        df.columns = [ cols // 24, cols % 24]
        cols = np.arange(len(df.columns))
        df.columns = [ cols // 24, cols % 24]
        df1 = df.stack().rename_axis((None, None)).reset_index(level=1, drop=True).reset_index()
        df1=df1.set_index('index')
        df1.columns = ['Actuals']
        return df1


    switcher = {
    1: coal,
    2: hydro,
    3: gas,
    4: biofuel,
    5: energy,
    6: wind,
    7: solar
    }
    
    def switch(switchNumber):
        return switcher.get(switchNumber)(paraGroup,startDateDASHED,endDateDASHED)
    return switch(switchNumber)
    

pd.options.mode.chained_assignment = None
avabcoalDF = sevenday_Availability(1,settings.todays_date(),settings.todays_date())
avabhydroDF = sevenday_Availability(2,settings.todays_date(),settings.todays_date())
avabgasDF = sevenday_Availability(3,settings.todays_date(),settings.todays_date())
avabbiofuelDF = sevenday_Availability(4,settings.todays_date(),settings.todays_date())
avabenergyDF = sevenday_Availability(5,settings.todays_date(),settings.todays_date())
avabwindDF = sevenday_Availability(6,settings.todays_date(),settings.todays_date())
avabsolarDF = sevenday_Availability(7,settings.todays_date(),settings.todays_date())


