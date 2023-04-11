#Settings will control the timers to get the correct date and time for the modules
import timeit
from datetime import tzinfo
import settings
import pandas as pd
import pytz
import datetime as dt
from datetime import timedelta

#Import the currents used to build the pipeline.
starttime = timeit.default_timer()

## Create dynamic timestamp for data truncation
import datetime as dt
def currentplusone():
    current = dt.datetime.now()
    current_td = dt.timedelta(
        hours = current.hour+1, 
        minutes = current.minute, 
        seconds = current.second, 
        microseconds = current.microsecond)
    to_hour = dt.timedelta(hours = round(current_td.total_seconds() // 3600))
    tohour = dt.datetime.combine(current, dt.time(0)) + to_hour
    print(tohour)
    #################################################################################################
    import Currents.CurrentAvailability.AESO_CurrentAvailability as aval_cu
    import Currents.CurrentDCR.NRGStreamApiDCRLastHour as DCR
    import Currents.CurrentDCR.NRGStreamApiDCRCurrentActiveSpinning as DCR_Spinning
    import Currents.CurrentDCR.NRGStreamApiDCRCurrentActiveSupplemental as DCR_Supplemental



    availableCoal = aval_cu.avabcoalDF
    availableEnergy = aval_cu.avabenergyDF
    availableGas = aval_cu.avabgasDF
    availableHydro = aval_cu.avabhydroDF
    availableWind = aval_cu.avabwindDF
    availableSolar = aval_cu.avabsolarDF
    availableOther = aval_cu.avabbiofuelDF
    DataFrameAvailable = pd.DataFrame()
    DataFrameAvailable['CoalAvailable'] = availableCoal['Actuals']
    DataFrameAvailable['EnergyAvailable'] = availableEnergy['Actuals']
    DataFrameAvailable['GasAvailable'] = availableGas['Actuals']
    DataFrameAvailable['HydroAvailable'] = availableHydro['Actuals']
    DataFrameAvailable['WindAvailable'] = availableWind['Actuals']
    DataFrameAvailable['SolarAvailable'] = availableSolar['Actuals']
    DataFrameAvailable['OtherAvailable'] = availableOther['Actuals']
    DataFrameAvailable = DataFrameAvailable.assign(Hour=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24])
    DataFrameAvailable['DateTime'] = DataFrameAvailable.index 
    DataFrameAvailable['DateTime'] = DataFrameAvailable['DateTime'] + pd.to_timedelta(DataFrameAvailable['Hour'], 'h')    
    DataFrameAvailable.index = DataFrameAvailable['DateTime']
    DataFrameAvailable= DataFrameAvailable.drop(['Hour'], axis=1)
    DataFrameAvailable= DataFrameAvailable.drop(['DateTime'], axis=1)
    DataFrameAvailable = DataFrameAvailable[DataFrameAvailable.index == tohour]
    #DataFrameAvailable = DataFrameAvailable.apply(pd.to_numeric, errors = 'coerce')

    DCRCurrent = pd.DataFrame(DCR.stream_data)
    DCRCurrent = DCRCurrent.reset_index(drop = False)
    DCRCurrent['Effective Date'] = (DCRCurrent['Effective Date'] + timedelta(hours=2))
    DCRCurrent.index  = DCRCurrent['Effective Date']
    DCRCurrent= DCRCurrent.drop(['Effective Date'], axis=1)
    print(DCRCurrent)



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

    print(DCRcurrentSupp)



    import Futures.FutureRenewable.NRGStreamAPIFutureRenewable as forecastSolar
    import Futures.FutureRenewable.NRGStreamAPIFutureRenewable as forecastWind

    GEN_DF = pd.DataFrame(forecastSolar.dfshortSolar)
    GEN_DF.rename(columns = {'Most Likely':'Forecast_Solar'}, inplace = True)
    GEN_DF['Forecast_wind'] = forecastWind.dfshortWind['Most Likely']


    #GEN_DF['Effective Date'] = pd.to_datetime(GEN_DF['Date/Time'])
    #GEN_DF.set_index('Effective Date',inplace=True)
    GEN_DF = GEN_DF[GEN_DF.index >= tohour]
    #GEN_DF= GEN_DF.drop(['Date/Time'], axis=1)
    print(GEN_DF)




    import Currents.CurrentLoad.NRGStreamApiAILNearFuture as AESO_LoadFuture
    AIL_DF = pd.DataFrame(AESO_LoadFuture.stream_data)
    AIL_DF.rename(columns = {'MW':'AIL_Demand'}, inplace = True)
    AIL_DF['Effective Date'] = pd.to_datetime(AIL_DF['Date/Time'])
    AIL_DF.set_index('Effective Date',drop=True ,inplace=True)
    #AIL_DF = AIL_DF.resample('60T').median()
    AIL_DF = AIL_DF[AIL_DF.index >= tohour]





    import Currents.CurrentInterchange.NRGStreamApiInterchange as Interchange_Current


    interchange_DF = pd.DataFrame(Interchange_Current.stream_data)
    interchange_DF.rename(columns = {'MW':'Interchange_Flowing'}, inplace = True)
    interchange_DF['Effective Date'] = pd.to_datetime(interchange_DF['Date/Time'])
    interchange_DF.set_index('Effective Date',drop=True ,inplace=True)
    #AIL_DF = AIL_DF.resample('60T').median()
    interchange_DF = interchange_DF[interchange_DF.index >= tohour]





    Current_HourFinalDF = pd.DataFrame(interchange_DF)
    Current_HourFinalDF['Load'] = AIL_DF['Load']
    Current_HourFinalDF['Solar'] = GEN_DF['Forecast_Solar']
    Current_HourFinalDF['Wind'] = GEN_DF['Forecast_wind']
    Current_HourFinalDF['DCR_SUP'] = DCRcurrentSupp['SUP_DCR']
    Current_HourFinalDF['DCR_SPIN'] = DCRcurrentSpin['SPIN_DCR']

    Current_HourFinalDF['Avab_Coal'] = DataFrameAvailable['CoalAvailable']
    Current_HourFinalDF['Avab_Energy'] = DataFrameAvailable['EnergyAvailable']
    Current_HourFinalDF['Avab_Gas'] = DataFrameAvailable['GasAvailable']
    Current_HourFinalDF['Avab_Hydro'] = DataFrameAvailable['HydroAvailable']
    Current_HourFinalDF['Avab_Other'] = DataFrameAvailable['OtherAvailable']
    #Current_HourFinalDF['Avab_Other'] = Current_HourFinalDF['Avab_Other'].fillna(method='ffill')
    #Current_HourFinalDF['Avab_Coal'] = Current_HourFinalDF['Avab_Coal'].fillna(method='ffill')
    #Current_HourFinalDF['Avab_Gas'] = Current_HourFinalDF['Avab_Gas'].fillna(method='ffill')
    #Current_HourFinalDF['Avab_Energy'] = Current_HourFinalDF['Avab_Energy'].fillna(method='ffill')
    #Current_HourFinalDF['Avab_Hydro'] = Current_HourFinalDF['Avab_Hydro'].fillna(method='ffill')
    #Current_HourFinalDF['Solar'] = Current_HourFinalDF['Solar'].fillna(method='ffill')
    #Current_HourFinalDF['Wind'] = Current_HourFinalDF['Wind'].fillna(method='ffill')
    #Current_HourFinalDF['Load'] = Current_HourFinalDF['Load'].fillna(method='ffill')
    Current_HourFinalDF = Current_HourFinalDF.apply(pd.to_numeric, errors = 'coerce')

    import Currents.CurrentInterchange.AESO_Interchange_ForwardHour as AESO_Interchange
    interchangeDFbcim = pd.DataFrame(AESO_Interchange.interchangeDFBCIM)
    interchangeDFbcex = pd.DataFrame(AESO_Interchange.interchangeDFBCEX)
    interchangeDFmatlim = pd.DataFrame(AESO_Interchange.interchangeDFMATLIM)
    interchangeDFmatlex = pd.DataFrame(AESO_Interchange.interchangeDFMATLEX)
    interchangeDFskim = pd.DataFrame(AESO_Interchange.interchangeDFSKIM)
    interchangeDFskex = pd.DataFrame(AESO_Interchange.interchangeDFSKEX)
    Historical_DF_INTERTIES = pd.DataFrame()
    Historical_DF_INTERTIES['Unutilized_BC_Imports'] = interchangeDFbcim['UnutilizedATC']
    Historical_DF_INTERTIES['Unutilized_BC_Exports'] = interchangeDFbcex['UnutilizedATC']

    Historical_DF_INTERTIES['Unutilized_MATL_Imports'] = interchangeDFmatlim['UnutilizedATC']
    Historical_DF_INTERTIES['Unutilized_MATL_Exports'] = interchangeDFmatlex['UnutilizedATC']

    Historical_DF_INTERTIES['Unutilized_SASK_Imports'] = interchangeDFskim['UnutilizedATC']
    Historical_DF_INTERTIES['Unutilized_SASK_Exports'] = interchangeDFskex['UnutilizedATC']

    Historical_DF_INTERTIES['Actual_BC_Imports'] = interchangeDFbcim['Actuals']
    Historical_DF_INTERTIES['Actual_BC_Exports'] = interchangeDFbcex['Actuals']

    Historical_DF_INTERTIES['Actual_MATL_Imports'] = interchangeDFmatlim['Actuals']
    Historical_DF_INTERTIES['Actual_MATL_Exports'] = interchangeDFmatlex['Actuals']

    Historical_DF_INTERTIES['Actual_SK_Imports'] = interchangeDFskim['Actuals']
    Historical_DF_INTERTIES['Actual_SK_Exports'] = interchangeDFskex['Actuals']


    Historical_DF_INTERTIES['x']= interchangeDFbcim['effectiveLocalTime']
    Historical_DF_INTERTIES['x'] = pd.to_datetime(Historical_DF_INTERTIES['x'], errors='coerce', utc=True).dt.strftime('%Y-%m-%d %H:%M')
    Historical_DF_INTERTIES['x'] = pd.to_datetime(Historical_DF_INTERTIES['x'])
    Historical_DF_INTERTIES.set_index('x',inplace=True,drop=True)



    Historical_DF_INTERTIES['ActualsSUMEXPOPTS'] = Historical_DF_INTERTIES['Actual_BC_Exports']+Historical_DF_INTERTIES['Actual_MATL_Exports']+Historical_DF_INTERTIES['Actual_SK_Exports']
    Historical_DF_INTERTIES['ActualsSUMIMPORTS'] = Historical_DF_INTERTIES['Actual_BC_Imports']+Historical_DF_INTERTIES['Actual_MATL_Imports']+Historical_DF_INTERTIES['Actual_SK_Imports']
    Historical_DF_INTERTIES['FinalDelta'] = Historical_DF_INTERTIES['ActualsSUMEXPOPTS'] - Historical_DF_INTERTIES['ActualsSUMIMPORTS']
    Historical_DF_INTERTIES = Historical_DF_INTERTIES[Historical_DF_INTERTIES.index >= tohour]
    Historical_DF_INTERTIES['ActualsSUMEXPOPTS'] = Historical_DF_INTERTIES['Actual_BC_Exports']+Historical_DF_INTERTIES['Actual_MATL_Exports']+Historical_DF_INTERTIES['Actual_SK_Exports']
    Historical_DF_INTERTIES['ActualsSUMIMPORTS'] = Historical_DF_INTERTIES['Actual_BC_Imports']+Historical_DF_INTERTIES['Actual_MATL_Imports']+Historical_DF_INTERTIES['Actual_SK_Imports']
    Historical_DF_INTERTIES['FinalDelta'] = Historical_DF_INTERTIES['ActualsSUMEXPOPTS'] - Historical_DF_INTERTIES['ActualsSUMIMPORTS']


    Current_HourFinalDF['Interchange_Flowing'] = Historical_DF_INTERTIES['FinalDelta']








    Current_HourFinalDF['DCR'] = Current_HourFinalDF['DCR_SPIN'] + Current_HourFinalDF['DCR_SUP'] - DCRCurrent['DCR']
    Current_HourFinalDF['FinalCurrentSupply'] = Current_HourFinalDF['Solar']+ Current_HourFinalDF['Wind'] + Current_HourFinalDF['Avab_Coal'] + Current_HourFinalDF['Avab_Energy'] + Current_HourFinalDF['Avab_Gas'] +Current_HourFinalDF['Avab_Hydro'] + Current_HourFinalDF['Avab_Other']
    Current_HourFinalDF['FinalCurrent'] = Current_HourFinalDF['FinalCurrentSupply'] - Current_HourFinalDF['Interchange_Flowing']  - Current_HourFinalDF['DCR'] - Current_HourFinalDF['Load'] 
    Current_HourFinalDF = Current_HourFinalDF[Current_HourFinalDF.index == tohour]
    Next_Current_HourFinalDF = Current_HourFinalDF
    Next_Current_HourFinalDF= Next_Current_HourFinalDF.drop(['DCR_SPIN'], axis=1)
    Next_Current_HourFinalDF= Next_Current_HourFinalDF.drop(['DCR_SUP'], axis=1)
    Next_Current_HourFinalDF= Next_Current_HourFinalDF.drop(['Date/Time'], axis=1)
    print(Next_Current_HourFinalDF)
    return Next_Current_HourFinalDF
    # # # # ################################################################################################
    # # # # ################################################################################################
    # # # # ################################################################################################
    # # # # ################################################################################################
    # # # # ################################################################################################
    # # # # ################################################################################################
    # # # # ################################################################################################
    # # # # ################################################################################################
    # # # # ################################################################################################
    # # # # ################################################################################################

    # import Historicals.HistoricalAvailability.NRGStreamApiCoalAvab as aval_hs_Coal
    # import Historicals.HistoricalAvailability.NRGStreamApiEnergyAvab as aval_hs_Energy
    # import Historicals.HistoricalAvailability.NRGStreamApiGasAvab as aval_hs_Gas
    # import Historicals.HistoricalAvailability.NRGStreamApiHydroAvab as aval_hs_Hydro
    # import Historicals.HistoricalAvailability.NRGStreamApiOtherAvab as aval_hs_Other
    # import Historicals.HistoricalAvailability.NRGStreamApiSolarAvab as aval_hs_Solar
    # import Historicals.HistoricalAvailability.NRGStreamApiWindAvab as aval_hs_Wind

    # Hist_Avalability = pd.DataFrame(aval_hs_Coal.stream_data)
    # Hist_Avalability.rename(columns = {'Availability Factor':'Coal Availability'}, inplace = True)
    # Hist_Avalability['Hydro'] = aval_hs_Hydro.stream_data['Availability Factor']
    # Hist_Avalability['Energy'] = aval_hs_Energy.stream_data['Availability Factor']
    # Hist_Avalability['Gas'] = aval_hs_Gas.stream_data['Availability Factor']
    # Hist_Avalability['Wind'] = aval_hs_Wind.stream_data['Availability Factor']
    # Hist_Avalability['Solar'] = aval_hs_Solar.stream_data['Availability Factor']
    # Hist_Avalability['Other'] = aval_hs_Other.stream_data['Availability Factor']

    # coefficientcoal = 1266
    # coefficientgas = 10836
    # coefficienthydro = 894
    # coefficientwind = 2269
    # coefficientsolar = 936
    # coefficientenergy = 50
    # coefficientother = 424
    # scalar = 100



    # Hist_Avalability['Coal Availability'] = Hist_Avalability['Coal Availability']
    # Hist_Avalability['Coal Availability'] = pd.to_numeric(Hist_Avalability['Coal Availability'])
    # Hist_Avalability['Coal Availability'] = Hist_Avalability['Coal Availability'] * coefficientcoal / scalar

    # Hist_Avalability['Hydro'] = Hist_Avalability['Hydro'] 
    # Hist_Avalability['Hydro'] = pd.to_numeric(Hist_Avalability['Hydro'])
    # Hist_Avalability['Hydro'] = Hist_Avalability['Hydro'] * coefficienthydro / scalar

    # Hist_Avalability['Energy'] = Hist_Avalability['Energy'] 
    # Hist_Avalability['Energy'] = pd.to_numeric(Hist_Avalability['Energy'])
    # Hist_Avalability['Energy'] = Hist_Avalability['Energy'] * coefficienthydro / scalar


    # Hist_Avalability['Gas'] = Hist_Avalability['Gas'] 
    # Hist_Avalability['Gas'] = pd.to_numeric(Hist_Avalability['Gas'])
    # Hist_Avalability['Gas'] = Hist_Avalability['Gas'] * coefficientgas / scalar

    # Hist_Avalability['Wind'] = Hist_Avalability['Wind'] 
    # Hist_Avalability['Wind'] = pd.to_numeric(Hist_Avalability['Wind']) 
    # Hist_Avalability['Wind'] = Hist_Avalability['Wind'] * coefficientwind / scalar


    # Hist_Avalability['Solar'] = Hist_Avalability['Solar'] 
    # Hist_Avalability['Solar'] = pd.to_numeric(Hist_Avalability['Solar'])
    # Hist_Avalability['Solar'] = Hist_Avalability['Solar'] * coefficientsolar / scalar


    # Hist_Avalability['Other'] = Hist_Avalability['Other'] 
    # Hist_Avalability['Other'] = pd.to_numeric(Hist_Avalability['Other'])
    # Hist_Avalability['Other'] = Hist_Avalability['Other'] * coefficientother / scalar



    # # // This dataframe is only the 24 hours of today. This should be saved at the top of every hour so it can be looked back on
    # # // Otherwise, need to put it into datetime.datetime format.
    # ################################################################################################




    # import Currents.CurrentDCR.NRGStreamApiDCRCoal as DCR_Coal
    # import Currents.CurrentDCR.NRGStreamApiDCRHydro as DCR_Hydro
    # import Currents.CurrentDCR.NRGStreamApiDCRGas as DCR_Gas
    # import Currents.CurrentDCR.NRGStreamApiDCREnergy as DCR_Energy
    # import Currents.CurrentDCR.NRGStreamApiDCRDual as DCR_Dual
    # import Currents.CurrentDCR.NRGStreamApiDCRSolar as DCR_Solar
    # import Currents.CurrentDCR.NRGStreamApiDCRWind as DCR_Wind
    # import Historicals.HistoricalDCR.NRGStreamApiDCRHourly as DCR_DCR

    # DCR_DF = pd.DataFrame(DCR_Coal.stream_data)
    # DCR_DF.rename(columns = {'MW':'Coal_DCR'}, inplace = True)
    # DCR_DF['DCR_Hydro'] = DCR_Hydro.stream_data['MW']
    # DCR_DF['DCR_Energy'] = DCR_Energy.stream_data['MW']
    # DCR_DF['DCR_Gas'] = DCR_Gas.stream_data['MW']
    # DCR_DF['DCR_Wind'] = DCR_Wind.stream_data['MW']
    # DCR_DF['DCR_Solar'] = DCR_Solar.stream_data['MW']
    # DCR_DF['DCR_Dual'] = DCR_Dual.stream_data['MW']

    # DCR_DCRDF = pd.DataFrame(DCR_DCR.stream_data)
    # DCR_DCRDF.rename(columns = {'MW':'HR_DCR'}, inplace = True)


    ######################################################################################################


    # import Currents.CurrentGeneration.NRGStreamApiGenCoal as genCOAL
    # import Currents.CurrentGeneration.NRGStreamApiGenHydro as genHydro
    # import Currents.CurrentGeneration.NRGStreamApiGenDual as genDual
    # import Currents.CurrentGeneration.NRGStreamApiGenEnergy as genEnergy
    # import Currents.CurrentGeneration.NRGStreamApiGenGas as genGas
    # import Currents.CurrentGeneration.NRGStreamApiGenSolar as genSolar
    # import Currents.CurrentGeneration.NRGStreamApiGenWind as genWind

    # GEN_DF = pd.DataFrame(genCOAL.stream_data)
    # GEN_DF.rename(columns = {'MW':'Gen_Coal'}, inplace = True)
    # GEN_DF['Gen_Dual'] = genDual.stream_data['MW']
    # GEN_DF['Gen_Energy'] = genEnergy.stream_data['MW']
    # GEN_DF['Gen_Gas'] = genGas.stream_data['MW']
    # GEN_DF['Gen_Hydro'] = genHydro.stream_data['MW']
    # GEN_DF['Gen_Solar'] = genSolar.stream_data['MW']
    # GEN_DF['Gen_Wind'] = genWind.stream_data['MW']




    # # #####################################################################################################



    # import Currents.CurrentLoad.NRGStreamApiAIL as AESO_Load
    # AIL_DF = pd.DataFrame(AESO_Load.stream_data)
    # AIL_DF.rename(columns = {'MW':'AIL_Demand'}, inplace = True)
    # AIL_DF['Effective Date'] = pd.to_datetime(AIL_DF['Date/Time'])
    # AIL_DF.set_index('Effective Date',drop=True ,inplace=True)
    # AIL_DF = AIL_DF.resample('60T').median()
    # AIL_DF = AIL_DF[AIL_DF.index <= tohour]


    # DCR_DCRDF['Effective Date'] = pd.to_datetime(DCR_DCRDF['Date/Time'])
    # DCR_DCRDF.set_index('Effective Date',drop=True ,inplace=True)
    # DCR_DCRDF= DCR_DCRDF.drop(['Date/Time'], axis=1)
    # DCR_DCRDF = DCR_DCRDF[DCR_DCRDF.index <= tohour]
    # # ######################################################################################################
    # import Currents.CurrentInterchange.AESO_Interchange as AESO_Interchange
    # interchangeDFbcim = pd.DataFrame(AESO_Interchange.interchangeDFBCIM)
    # interchangeDFbcex = pd.DataFrame(AESO_Interchange.interchangeDFBCEX)
    # interchangeDFmatlim = pd.DataFrame(AESO_Interchange.interchangeDFMATLIM)
    # interchangeDFmatlex = pd.DataFrame(AESO_Interchange.interchangeDFMATLEX)
    # interchangeDFskim = pd.DataFrame(AESO_Interchange.interchangeDFSKIM)
    # interchangeDFskex = pd.DataFrame(AESO_Interchange.interchangeDFSKEX)
    # Historical_DF_INTERTIES = pd.DataFrame()
    # Historical_DF_INTERTIES['Unutilized_BC_Imports'] = interchangeDFbcim['UnutilizedATC']
    # Historical_DF_INTERTIES['Unutilized_BC_Exports'] = interchangeDFbcex['UnutilizedATC']

    # Historical_DF_INTERTIES['Unutilized_MATL_Imports'] = interchangeDFmatlim['UnutilizedATC']
    # Historical_DF_INTERTIES['Unutilized_MATL_Exports'] = interchangeDFmatlex['UnutilizedATC']

    # Historical_DF_INTERTIES['Unutilized_SASK_Imports'] = interchangeDFskim['UnutilizedATC']
    # Historical_DF_INTERTIES['Unutilized_SASK_Exports'] = interchangeDFskex['UnutilizedATC']

    # Historical_DF_INTERTIES['Actual_BC_Imports'] = interchangeDFbcim['Actuals']
    # Historical_DF_INTERTIES['Actual_BC_Exports'] = interchangeDFbcex['Actuals']

    # Historical_DF_INTERTIES['Actual_MATL_Imports'] = interchangeDFmatlim['Actuals']
    # Historical_DF_INTERTIES['Actual_MATL_Exports'] = interchangeDFmatlex['Actuals']

    # Historical_DF_INTERTIES['Actual_SK_Imports'] = interchangeDFskim['Actuals']
    # Historical_DF_INTERTIES['Actual_SK_Exports'] = interchangeDFskex['Actuals']


    # Historical_DF_INTERTIES['x']= interchangeDFbcim['effectiveLocalTime']
    # Historical_DF_INTERTIES['x'] = pd.to_datetime(Historical_DF_INTERTIES['x'], errors='coerce', utc=True).dt.strftime('%Y-%m-%d %H:%M')
    # Historical_DF_INTERTIES['x'] = pd.to_datetime(Historical_DF_INTERTIES['x'])
    # Historical_DF_INTERTIES.set_index('x',inplace=True,drop=True)

    # # #######################################################################################################
    # import Currents.CurrentSMP.NRGStreamApiSMP as AESO_SMP

    # # # #####################################################################################################




    # # ###########################################
    # Historical_DF = pd.DataFrame()
    # Historical_DF['AIL_Demand'] = AIL_DF['AIL_Demand']
    # Historical_DF['Gen_Solar'] = GEN_DF['Gen_Solar']
    # Historical_DF['Gen_Wind'] = GEN_DF['Gen_Wind']
    # #Historical_DF.index = GEN_DF.index






    # #####################################################################################################################################
    # Hist_Avalability['Total Historical Generation Available'] = Hist_Avalability['Coal Availability']+Hist_Avalability['Hydro']+Hist_Avalability['Energy']+Hist_Avalability['Gas']+Hist_Avalability['Other']
    # Hist_Avalability['Effective Date'] = pd.to_datetime(Hist_Avalability['Date/Time'])
    # Hist_Avalability.set_index('Effective Date',inplace=True)
    # Hist_Avalability = Hist_Avalability[Hist_Avalability.index <= tohour]
    # print(Hist_Avalability)
    # ##This part of the code is intended to make the historical availability stop right at the last flowing hour. So if it 11:05AM, the data is truncated at x/y/z date at 11:00. This is 
    # ##done so that it marries nicely with the historical wind and solar generation data. Flowing hour and future availability will be recast seperately into a seperate dataframe.

    # ##Apparently have to do the same thing with exports and imports. Sign convention is that imports are always negative. So to keep with this, we will minus (a negative number) so that
    # ##supply increases when alberta is importing. 

    # Historical_DF_INTERTIES['ActualsSUMEXPOPTS'] = Historical_DF_INTERTIES['Actual_BC_Exports']+Historical_DF_INTERTIES['Actual_MATL_Exports']+Historical_DF_INTERTIES['Actual_SK_Exports']
    # Historical_DF_INTERTIES['ActualsSUMIMPORTS'] = Historical_DF_INTERTIES['Actual_BC_Imports']+Historical_DF_INTERTIES['Actual_MATL_Imports']+Historical_DF_INTERTIES['Actual_SK_Imports']
    # Historical_DF_INTERTIES['FinalDelta'] = Historical_DF_INTERTIES['ActualsSUMEXPOPTS'] - Historical_DF_INTERTIES['ActualsSUMIMPORTS']
    # Historical_DF_INTERTIES = Historical_DF_INTERTIES[Historical_DF_INTERTIES.index <= tohour]


    # GenerationRenewables = GEN_DF
    # GenerationRenewables['Effective Date'] = pd.to_datetime(GenerationRenewables['Date/Time'])
    # GenerationRenewables.set_index('Effective Date',inplace=True)
    # GenerationRenewables = GenerationRenewables.resample('60T').median()





    # #DCR_DCRDF['Effective Date'] = pd.to_datetime(DCR_DCRDF['Date/Time'])
    # #DCR_DCRDF.set_index('Effective Date',drop=True ,inplace=True)
    # #DCR_DCRDF= DCR_DCRDF.drop(['Date/Time'], axis=1)
    # #DCR_DCRDF = DCR_DCRDF[DCR_DCRDF.index <= tohour]
    # DCR_DCRDF['HR_DCR'] = DCR_DCRDF['HR_DCR'].astype(float)












    # dfHistoricalFinal = pd.merge(GenerationRenewables, Hist_Avalability, left_index=True, right_index=True)


    # dfHistoricalFinal= dfHistoricalFinal.drop(columns = ['Date/Time'])
    # dfHistoricalFinal['Delta Interchange'] = Historical_DF_INTERTIES['FinalDelta']
    # dfHistoricalFinal['Supply_sans_DCR'] = dfHistoricalFinal['Total Historical Generation Available'] - dfHistoricalFinal['Delta Interchange']
    # dfHistoricalFinal['Historical DCR'] = DCR_DCRDF['HR_DCR']
    # dfHistoricalFinal['Historical DCR'] = dfHistoricalFinal['Historical DCR'].fillna(method='ffill')
    # dfHistoricalFinal['FinalHistoricalSupply'] = dfHistoricalFinal['Supply_sans_DCR'] - dfHistoricalFinal['Historical DCR']
    # dfHistoricalFinal['Load'] = AIL_DF['AIL_Demand']




    # # # I think this should be called past so I can control how often it triggers.
    # # # # Remember there is no gas derate option built in yet

    # print("The time difference is :", timeit.default_timer() - starttime)
currentplusone()