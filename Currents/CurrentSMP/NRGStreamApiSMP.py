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

class NRGStreamApiSMP:

    def __init__(self,username=None,password=None):
            self.username = 'suncor2'
            self.password = 'anisoTropical308'                
            self.server = 'api.nrgstream.com'        
            self.tokenPath = '/api/security/token'
            self.releasePath = '/api/ReleaseToken'
            self.tokenPayload = f'grant_type=password&username={self.username}&password={self.password}'
            self.tokenExpiry = datetime.now() - timedelta(seconds=60)
            self.accessToken = ""
                 

    def getToken(self):
            try:
                if self.isTokenValid() == False:                             
                    headers = {"Content-type": "application/x-www-form-urlencoded"}      
                    # Connect to API server to get a token
                    context = ssl.create_default_context(cafile=certifi.where())
                    conn = http.client.HTTPSConnection(self.server,context=context)
                    conn.request('POST', self.tokenPath, self.tokenPayload, headers)
                    res = conn.getresponse()                
                    res_code = res.code
                    # Check if the response is good
                    
                    if res_code == 200:
                        res_data = res.read()
                        # Decode the token into an object
                        jsonData = json.loads(res_data.decode('utf-8'))
                        self.accessToken = jsonData['access_token']                         
                        # Calculate new expiry date
                        self.tokenExpiry = datetime.now() + timedelta(seconds=jsonData['expires_in'])                        
                        #print('token obtained')
                        #print(self.accessToken)
                    else:
                        res_data = res.read()
                        print(res_data.decode('utf-8'))
                    conn.close()                          
            except Exception as e:
                print("getToken: " + str(e))
                # Release token if an error occured
                self.releaseToken()      
    def releaseToken(self):
            try:            
                headers = {}
                headers['Authorization'] = f'Bearer {self.accessToken}'            
                context = ssl.create_default_context(cafile=certifi.where())
                conn = http.client.HTTPSConnection(self.server,context=context)
                conn.request('DELETE', self.releasePath, None, headers)  
                res = conn.getresponse()
                res_code = res.code
                if res_code == 200:   
                    # Set expiration date back to guarantee isTokenValid() returns false                
                    self.tokenExpiry = datetime.now() - timedelta(seconds=60)
                    #print('token released')            
            except Exception as e:
                print("releaseToken: " + str(e))
                        
    def isTokenValid(self):
            if self.tokenExpiry==None:
                return False
            elif datetime.now() >= self.tokenExpiry:            
                return False
            else:
                return True            

    def GetStreamDataByStreamId(self,streamIds, fromDate, toDate, dataFormat='csv', dataOption=''):
            stream_data = "" 
            # Set file format to csv or json            
            DataFormats = {}
            DataFormats['csv'] = 'text/csv'
            DataFormats['json'] = 'Application/json'
            
            try:                            
                for streamId in streamIds:            
                    # Get an access token            
                    self.getToken()    
                    if self.isTokenValid():
                        # Setup the path for data request. Pass dates in via function call
                        path = f'/api/StreamData/{streamId}'
                        if fromDate != '' and toDate != '':
                            path += f'?fromDate={fromDate.replace(" ", "%20")}&toDate={toDate.replace(" ", "%20")}'
                        if dataOption != '':
                            if fromDate != '' and toDate != '':
                                path += f'&dataOption={dataOption}'        
                            else:
                                path += f'?dataOption={dataOption}'        
                        
                        # Create request header
                        headers = {}            
                        headers['Accept'] = DataFormats[dataFormat]
                        headers['Authorization']= f'Bearer {self.accessToken}'
                        
                        # Connect to API server
                        context = ssl.create_default_context(cafile=certifi.where())
                        conn = http.client.HTTPSConnection(self.server,context=context)
                        conn.request('GET', path, None, headers)
                        res = conn.getresponse()        
                        res_code = res.code                    
                        if res_code == 200:   
                            try:
                                print(f'{datetime.now()} Outputing stream {path} res code {res_code}')
                                # output return data to a text file            
                                if dataFormat == 'csv':
                                    stream_data += res.read().decode('utf-8').replace('\r\n','\n') 
                                elif dataFormat == 'json':
                                    stream_data += json.dumps(json.loads(res.read().decode('utf-8')), indent=2, sort_keys=False)
                                conn.close()

                            except Exception as e:
                                print(str(e))            
                                self.releaseToken()
                                return None  
                        else:
                            print(str(res_code) + " - " + str(res.reason) + " - " + str(res.read().decode('utf-8')))
                        
                    self.releaseToken()   
                    # Wait 1 second before next request
                    time.sleep(1)
                return stream_data        
            except Exception as e:
                print(str(e))    
                self.releaseToken()
                return None
    def csvStreamToPandas(self, streamData):
        # split lines of return string from api
        streamData = streamData.split("\n")
        
        # remove empty elements from list
        streamData = [x for x in streamData if len(x) > 0] 
        
        # remove header data
        streamData = [x for x in streamData if x[0] != '#'] 
                     
        # split elements into lists of lists                     
        streamData = [x.split(",") for x in streamData] 
        
        # create dataframe
        df = pd.DataFrame(streamData[1:], columns=streamData[0]) 
        
        return df

try:    
    # Authenticate with your NRGSTREAM username and password contained in credentials.txt, file format = username,password 
    #f = open(r"C:\Users\micoconnell\OneDrive - Suncor Energy Inc\Desktop\PythonRepo\AnnualDetectionChange\credentials.txt", "r")
    
    
    #credentials = f.readline().split(',')
    reedus = settings.smp_API
    #reedus = int(reedus)
    #f.close()
    nrgStreamApi = NRGStreamApiSMP(settings.USERNAME,settings.PASSWORD)         
    # Date range for your data request
    # Date format must be 'mm/dd/yyyy hh:ss'
    fromDateStr = settings.prev7Days_Date()
    toDateStr = settings.todayDays_Date()

    # Specify output format - 'csv' or 'json'
    dataFormat = 'csv'
    
    # Convert streams to Pandas dataframes
    # Only compatible with getByStream and getByFolder
    dataFrameConvert = True
    
    # Data Option
    dataOption = ''
    
    # Output from the API request is written to the stream_data variable
    stream_data = ""

    # Output from the API request is written to the streamList variable
    streamList = ""
    
    # Output from the API request is written to the folderList variable
    folderList = ""
    
    # Output from the API request is written to the groupExtractsList variable
    groupExtractsList = ""
    

    # Change to True to get streams by Stream Id
    getByStream = True    
    if getByStream:
        # Pass in individual stream id
        streamIds = [reedus]       
        # Or pass in list of stream ids
        #streamIds = [139308, 3, 225, 4117, 17, 545, 40034]         
        stream_data = nrgStreamApi.GetStreamDataByStreamId(streamIds, fromDateStr, toDateStr, dataFormat, dataOption) 
        
        if(dataFrameConvert and dataFormat == 'csv'):
            if(len(streamIds) > 1):
                print('Please only convert 1 stream to a Pandas dataframe at a time')
            else:
                stream_data = nrgStreamApi.csvStreamToPandas(stream_data)
        
        
    # Change to True to get streams by Folder Id
    getByFolder = False
    if getByFolder:
        # Pass in individual folder id
        folderId = 9
        nrgStreamApi.GetStreamDataByFolderId(folderId, fromDateStr, toDateStr, dataFormat)

    # Change to True to retrieve a list of data options available for a given stream
    getStreamDataOptions = False
    if getStreamDataOptions:          
        # Pass in a list of streamIds to get the shapes available for each
        # These shapes can be passed to StreamData endpoint as 'displayOption' to retrieve only that shape
        streamIds = [2270]    
        streamDataOptions = nrgStreamApi.StreamDataOptions(streamIds, dataFormat)
        print(streamDataOptions)
  
     
except Exception as e:
    print(str(e))


