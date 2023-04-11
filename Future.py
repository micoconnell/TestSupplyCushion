

#Settings will control the timers to get the correct date and time for the modules
import timeit
from datetime import tzinfo
import settings
import pandas as pd
import pytz
import datetime as dt
#Import the currents used to build the pipeline.


## Create dynamic timestamp for data truncation
import datetime as dt
def futures():
    current = dt.datetime.now()
    current_td = dt.timedelta(
        hours = current.hour +2, 
        minutes = current.minute, 
        seconds = current.second, 
        microseconds = current.microsecond)
    to_hour = dt.timedelta(hours = round(current_td.total_seconds() // 3600))
    tohour = dt.datetime.combine(current, dt.time(0)) + to_hour
    print(tohour)
    #################################################################################################




    # # # # ################################################################################################


    # // This dataframe is only the 24 hours of today. This should be saved at the top of every hour so it can be looked back on
    # // Otherwise, need to put it into datetime.datetime format.
    ################################################################################################




    import Currents.CurrentDCR.NRGStreamApiDCRCurrentActiveSpinning as DCR_Spinning
    import Currents.CurrentDCR.NRGStreamApiDCRCurrentActiveSupplemental as DCR_Supplemental



    DCRcurrentSpin = pd.DataFrame(DCR_Spinning.stream_data)
    DCRcurrentSpin.rename(columns = {'MW':'SPIN_DCR'}, inplace = True)
    DCRcurrentSpin['Effective Date'] = pd.to_datetime(DCRcurrentSpin['Date/Time'])
    DCRcurrentSpin.set_index('Effective Date',drop=True ,inplace=True)
    DCRcurrentSpin= DCRcurrentSpin.drop(['Date/Time'], axis=1)
    DCRcurrentSpin = DCRcurrentSpin[DCRcurrentSpin.index >= tohour]
    DCRcurrentSpin['SPIN_DCR'] = DCRcurrentSpin['SPIN_DCR'].astype(float)



    DCRcurrentSupp = pd.DataFrame(DCR_Supplemental.stream_data)
    DCRcurrentSupp.rename(columns = {'MW':'SUP_DCR'}, inplace = True)
    DCRcurrentSupp['Effective Date'] = pd.to_datetime(DCRcurrentSupp['Date/Time'])
    DCRcurrentSupp.set_index('Effective Date',drop=True ,inplace=True)
    DCRcurrentSupp= DCRcurrentSupp.drop(['Date/Time'], axis=1)
    DCRcurrentSupp = DCRcurrentSupp[DCRcurrentSupp.index >= tohour]
    DCRcurrentSupp['SUP_DCR'] = DCRcurrentSupp['SUP_DCR'].astype(float)
    #DCRcurrent['HR_DCR'] = DCRcurrent.resample('60T').median()

    ############################################################################################
    import Futures.FutureRenewable.NRGStreamAPIFutureRenewable as Forecast

    short_TermWind = Forecast.dfshortWind
    short_TermSolar = Forecast.dfshortSolar
    long_TermSolar = Forecast.dflongSolar
    long_TermWind = Forecast.dflongWind

    #Renewable_Forecasts = pd.DataFrame(Forecast.dfshortWind['Most Likely'])
    #Renewable_Forecasts['Wind_Short'] = Renewable_Forecasts['Most Likely']
    #Renewable_Forecasts['Solar_Short'] = Forecast.dfshortSolar['Most Likely']
    #correctTiming = Renewable_Forecasts.last_valid_index()


    Renewable_Forecasts_Long = pd.DataFrame(Forecast.dflongWind['Most Likely'])
    Renewable_Forecasts_Long ['Wind_Long'] = Renewable_Forecasts_Long['Most Likely']
    Renewable_Forecasts_Long ['Solar_Long'] = Forecast.dflongSolar['Most Likely']

    #Renewable_Forecasts_Long = Renewable_Forecasts_Long[Renewable_Forecasts_Long.index > correctTiming]






    # # ####################################################################################################


    import Futures.FutureLoad.NRGStreamApiLoad as AESO_Load
    AIL_DFFuture = pd.DataFrame(AESO_Load.stream_data)
    AIL_DFFuture.rename(columns = {'MW':'AIL_Demand'}, inplace = True)
    AIL_DFFuture['Effective Date'] = pd.to_datetime(AIL_DFFuture['Date/Time'])
    AIL_DFFuture.set_index('Effective Date',drop=True ,inplace=True)
    AIL_DF = AIL_DFFuture.resample('60T').median()
    #AIL_DF = AIL_DF[AIL_DF.index <= tohour]




    # # #####################################################################################################

    import Futures.FutureAvailability.AESO_FutureAvailability as available_Future

    dfFinalAvailbilitycu = available_Future.dfAvailableFuture

    #dfFinalAvailbility.index = Renewable_Forecasts_Long.index
    #dfFinalAvailbility = pd.to_numeric(dfFinalAvailbility,errors='ignore')
    # # ######################################################################################################
    import Futures.FutureInterchange.AESO_FutureInterchange as AESO_Interchange
    interchangeDFbcim = pd.DataFrame(AESO_Interchange.interchangeDFBCIM)
    interchangeDFbcex = pd.DataFrame(AESO_Interchange.interchangeDFBCEX)
    interchangeDFmatlim = pd.DataFrame(AESO_Interchange.interchangeDFMATLIM)
    interchangeDFmatlex = pd.DataFrame(AESO_Interchange.interchangeDFMATLEX)
    interchangeDFskim = pd.DataFrame(AESO_Interchange.interchangeDFSKIM)
    interchangeDFskex = pd.DataFrame(AESO_Interchange.interchangeDFSKEX)
    FutureFinalInterchange = pd.DataFrame(index=interchangeDFbcim.index)
    FutureFinalInterchange['Delta_Interchange'] = -interchangeDFbcim['Actuals'] + interchangeDFbcex['Actuals'] - interchangeDFmatlim['Actuals'] + interchangeDFmatlex['Actuals'] - interchangeDFskim['Actuals'] + interchangeDFskex['Actuals']
    FutureFinalInterchange['effectiveLocalTime']= interchangeDFbcim['effectiveLocalTime']
    FutureFinalInterchange['effectiveLocalTime'] = pd.to_datetime(FutureFinalInterchange['effectiveLocalTime'], errors='coerce', utc=True).dt.strftime('%Y-%m-%d %H:%M')
    FutureFinalInterchange['effectiveLocalTime'] = pd.to_datetime(FutureFinalInterchange['effectiveLocalTime'])

    FutureFinalInterchange.set_index('effectiveLocalTime',inplace=True,drop=True)
    ############################################################################################################
    # # #######################################################################################################
    # import Currents.CurrentSMP.NRGStreamApiSMP as AESO_SMP

    # # # #####################################################################################################
    FinalFutureAll = pd.DataFrame(FutureFinalInterchange)
    FinalFutureAll['Load'] = AIL_DF['Load']
    FinalFutureAll['Gen_Solar'] = Renewable_Forecasts_Long['Solar_Long']
    FinalFutureAll['Gen_Wind'] = Renewable_Forecasts_Long['Most Likely']
    FinalFutureAll['DCR_SPIN'] = DCRcurrentSpin['SPIN_DCR']
    FinalFutureAll['DCR_SUPP'] = DCRcurrentSupp['SUP_DCR']
    FinalFutureAll = FinalFutureAll.iloc[:168,:]
    FinalFutureAll['coal_Avab'] = dfFinalAvailbilitycu['ActualCoal']
    FinalFutureAll['hydro_Avab'] = dfFinalAvailbilitycu['ActualHydro']
    FinalFutureAll['gas_Avab'] = dfFinalAvailbilitycu['ActualGas']
    FinalFutureAll['Solar_Availability'] = dfFinalAvailbilitycu['ActualSolar']
    FinalFutureAll['Wind_Availability'] = dfFinalAvailbilitycu['ActualWind']
    FinalFutureAll['avab_Other'] = dfFinalAvailbilitycu['ActualBiofuel']
    FinalFutureAll['energy_Avab'] = dfFinalAvailbilitycu['ActualEnergy']
    FinalFutureAll['DCR'] = FinalFutureAll['DCR_SPIN'] + FinalFutureAll['DCR_SUPP']
    FinalFutureAll= FinalFutureAll.drop(['DCR_SPIN'], axis=1)
    FinalFutureAll= FinalFutureAll.drop(['DCR_SUPP'], axis=1)
    FinalFutureAll['FinalHistoricalSupply'] = FinalFutureAll['Gen_Solar']+ FinalFutureAll['Gen_Wind'] + FinalFutureAll['coal_Avab'] +FinalFutureAll['hydro_Avab'] + FinalFutureAll['gas_Avab'] + FinalFutureAll['avab_Other'] + FinalFutureAll['energy_Avab']
    FinalFutureAll['supply_Cushion'] = FinalFutureAll['FinalHistoricalSupply'] - FinalFutureAll['Delta_Interchange'] - FinalFutureAll['Load'] - FinalFutureAll['DCR']
    FinalFutureAll = FinalFutureAll[FinalFutureAll.index >= tohour]


    print(FinalFutureAll)
    return FinalFutureAll
futures()