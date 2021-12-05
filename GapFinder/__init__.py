import datetime as dt
import logging
import requests

import pandas as pd
import yfinance as yf
import numpy as np
import pandas_datareader as pdr

from typing import List, Dict

from azure.storage.blob import *
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

from azure.cosmosdb.table.tableservice import TableService

import azure.functions as func

from io import StringIO

def main(mytimer: func.TimerRequest) -> None:

    account_name = 'mkccsvs'
    account_Key = 'N33wcyz+gndJj/laNFLy4mdG7yhE+5TTkuBD9DCCnXN5F4v/bAz71hrn2+UZNtvIsx1nkS2VPw8rJI/IZnfBmA=='

    gap_stocks = []

    # pass a ticker and see if it has been in an uptrend for a while and then started falling down
    # Get three points -> A - recent , B- mid , C - past
    # A should be less than B but greater than C 
    # A should have fallen less than 40%(distance) from B towards C

    start = dt.datetime.now() + dt.timedelta(days= -50)

    now = dt.datetime.now()
                  
    tickerdf = pd.read_csv('https://mkccsvs.blob.core.windows.net/shortlist/SNP.csv?sp=r&st=2021-11-22T00:00:00Z&se=2023-02-01T04:28:51Z&sv=2020-08-04&sr=b&sig=TNZG8Ps0bHV9Nf802eXc796UrV0gyjJEQOph6qHGPO4%3D',names=['Ticker'])

    tickers =  tickerdf.Ticker.to_list()


    # Find daily gap ups, which are greater than specific percent and are not filled

    def ret_gaps_stocks(ticker,start,end,gap_Percent,current_price):

        #get real time current price here
        current_price = 28

        data = pd.DataFrame(pdr.get_data_yahoo(ticker,start,end)[['Open','Close','High','Low']])
        data['Day_Of_week'] = data.index.dayofweek
        data['Previous_Close'] = data['Close'].shift(1)
        data = data.dropna()

        data['Percent_Difference'] = (data.Open - data.Previous_Close) / data.Previous_Close * 100

        gapdf = data[data['Percent_Difference'] > 7]

        #gapdf.reset_index(inplace=True)

        #gapdf['Date'] = gapdf['Date'].dt.date

        #print(gapdf)


        records = gapdf.to_records(index=True)
        
        records = list(records)
        
        #Dict of gaps
        # 
        gaps =  {}
        
        #item is tuple from list of tuples
        for item in records:

            # check if the current price is above the gapped up Top price 
            #'Gap_Top':item[1],'Gap_Bottom':item[2]

            if item[1] < current_price:

                #check if the gap has been filled earlier
                #check if open, close high or low has visted the gap range after gap has formed


                data = data[data.index > item[0]]

                closelist =  min(data.Close.min(),data.Open.min(),data.High.min(),data.Low.min())
                print(closelist)
                print(data)
                print(item[0])

                gaps[item[0]] = {'Gap_Top':item[1],'Gap_Bottom':item[2]}

        #print(gaps)


    ret_gaps_stocks('CHPT',start,now,5,28)



    









    






