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



##AESO API Date calls are always in the format of YYYYMMDD and passed as a string.


def interties_ScrapeForecast(i,k,n,switchNumber):
    i = i
    k = k
    n = n
    switchNumber = switchNumber
    csv_url = "https://itc.aeso.ca/itc/public/api/v2/interchange?startDate={i}&endDate={k}&pageNo=1&pageSize={n}".format(i=i,k=k,n=n)
    req = requests.get(csv_url)
    dd=json.loads(req.text)
    def BC_Import(dd):
        df_BCimport=pd.DataFrame(dd['return']['BcIntertie']['Allocations'])
        BCImport_Raw= df_BCimport['import'].apply(pd.Series)
        BCImport_Raw = BCImport_Raw.set_index('effectiveLocalTime')
        BCImport_Raw['Actuals'] = BCImport_Raw[['atc','grossOffer']].min(axis=1)
        BCImport_Raw['UnutilizedATC'] = (BCImport_Raw['atc'] - BCImport_Raw['grossOffer']).clip(0)
        return BCImport_Raw
    def BC_Export(dd):
        df_BCexport=pd.DataFrame(dd['return']['BcIntertie']['Allocations'])
        BCExport_Raw= df_BCexport['export'].apply(pd.Series)
        BCExport_Raw = BCExport_Raw.set_index('effectiveLocalTime')
        BCExport_Raw['Actuals'] = BCExport_Raw[['atc','grossOffer']].min(axis=1)
        BCExport_Raw['UnutilizedATC'] = (BCExport_Raw['atc'] - BCExport_Raw['grossOffer']).clip(0)
        return BCExport_Raw
    def MATL_Import(dd):
        df_MATLImport=pd.DataFrame(dd['return']['MatlIntertie']['Allocations'])
        MATLImport_Raw= df_MATLImport['import'].apply(pd.Series)
        MATLImport_Raw = MATLImport_Raw.set_index('effectiveLocalTime')
        MATLImport_Raw['Actuals'] = MATLImport_Raw[['atc','grossOffer']].min(axis=1)
        MATLImport_Raw['UnutilizedATC'] = (MATLImport_Raw['atc'] - MATLImport_Raw['grossOffer']).clip(0)
        return MATLImport_Raw
    def MATL_Export(dd):
        df_MATLExport=pd.DataFrame(dd['return']['MatlIntertie']['Allocations'])
        MATLExport_Raw= df_MATLExport['export'].apply(pd.Series)
        MATLExport_Raw = MATLExport_Raw.set_index('effectiveLocalTime')
        MATLExport_Raw['Actuals'] = MATLExport_Raw[['atc','grossOffer']].min(axis=1)
        MATLExport_Raw['UnutilizedATC'] = (MATLExport_Raw['atc'] - MATLExport_Raw['grossOffer']).clip(0)
        return MATLExport_Raw
    def SK_Import(dd):
        df_SkImport=pd.DataFrame(dd['return']['SkIntertie']['Allocations'])
        SKImport_Raw= df_SkImport['import'].apply(pd.Series)
        SKImport_Raw = SKImport_Raw.set_index('effectiveLocalTime')
        SKImport_Raw['Actuals'] = SKImport_Raw[['atc','grossOffer']].min(axis=1)
        SKImport_Raw['UnutilizedATC'] = (SKImport_Raw['atc'] - SKImport_Raw['grossOffer']).clip(0)
        return SKImport_Raw
    def SK_Export(dd):
        df_SkExport=pd.DataFrame(dd['return']['SkIntertie']['Allocations'])
        SKExport_Raw= df_SkExport['export'].apply(pd.Series)
        SKExport_Raw = SKExport_Raw.set_index('effectiveLocalTime')
        SKExport_Raw['Actuals'] = SKExport_Raw[['atc','grossOffer']].min(axis=1)
        SKExport_Raw['UnutilizedATC'] = (SKExport_Raw['atc'] - SKExport_Raw['grossOffer']).clip(0)
        return SKExport_Raw

    switcher = {
    1: BC_Import,
    2: BC_Export,
    3: MATL_Import,
    4: MATL_Export,
    5: SK_Import,
    6: SK_Export,
    }
    
    def switch(switchNumber):
        return switcher.get(switchNumber)(dd)
    

    return switch(switchNumber)


pd.options.mode.chained_assignment = None

interchangeDFBCIM = interties_ScrapeForecast(settings.past7_DateAESOFORMAT(),settings.current_DateAESOFORMAT(),1,1)
interchangeDFBCIM= interchangeDFBCIM.reset_index()
interchangeDFBCEX = interties_ScrapeForecast(settings.past7_DateAESOFORMAT(),settings.current_DateAESOFORMAT(),1,2)
interchangeDFBCEX = interchangeDFBCEX.reset_index()
interchangeDFMATLIM = interties_ScrapeForecast(settings.past7_DateAESOFORMAT(),settings.current_DateAESOFORMAT(),1,3)
interchangeDFMATLIM= interchangeDFMATLIM.reset_index()
interchangeDFMATLEX = interties_ScrapeForecast(settings.past7_DateAESOFORMAT(),settings.current_DateAESOFORMAT(),1,4)
interchangeDFMATLEX= interchangeDFMATLEX.reset_index()
interchangeDFSKIM = interties_ScrapeForecast(settings.past7_DateAESOFORMAT(),settings.current_DateAESOFORMAT(),1,5)
interchangeDFSKIM = interchangeDFSKIM.reset_index()
interchangeDFSKEX = interties_ScrapeForecast(settings.past7_DateAESOFORMAT(),settings.current_DateAESOFORMAT(),1,6)
interchangeDFSKEX = interchangeDFSKEX.reset_index()




