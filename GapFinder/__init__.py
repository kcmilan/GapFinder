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

    start = dt.datetime.now() + dt.timedelta(days= -80)

    now = dt.datetime.now()

    gap_percent = 4
                  
    tickerdf = pd.read_csv('https://mkccsvs.blob.core.windows.net/shortlist/SNP.csv?sp=r&st=2021-11-22T00:00:00Z&se=2023-02-01T04:28:51Z&sv=2020-08-04&sr=b&sig=TNZG8Ps0bHV9Nf802eXc796UrV0gyjJEQOph6qHGPO4%3D',names=['Ticker'])

    tickers =  tickerdf.Ticker.to_list()

    print(tickers)

  
    # Find daily gap ups, which are greater than specific percent and are not filled

    def ret_gaps_stocks(ticker,start,end,gap_Percent):

        #get real time current price here

        data = pd.DataFrame(pdr.get_data_yahoo(ticker,start,end)[['Open','Close','High','Low']])

        current_price = data.tail(1)['Close'].item()

        #print(current_price)

        data['Day_Of_week'] = data.index.dayofweek
        data['Previous_Close'] = data['Close'].shift(1)
        data = data.dropna()

        data['Percent_Difference'] = (data.Open - data.Previous_Close) / data.Previous_Close * 100
        
        #print(data)

        gapdf = data[data['Percent_Difference'] > gap_percent]

        #gapdf.reset_index(inplace=True)

        #gapdf['Date'] = gapdf['Date'].dt.date

        #print(gapdf)

        records = gapdf.to_records(index=True)
        
        records = list(records)

        #print(records)
        
        #Dict of gaps
        # 
        gaps =  {}
        
        #item is tuple from list of tuples
        for idx, item in enumerate(records):
            
            #print(item)
            # check if the current price is above the gapped up Top price 
            #'Gap_Top':item[1],'Gap_Bottom':item[2]

            if item[1] < current_price:

                #check if the gap has been filled earlier
                #check if open, close high or low has visted the gap range after gap has formed


                data = data[data.index > item[0]]

                gap_revisit_price =  min(data.Close.min(),data.Open.min(),data.High.min(),data.Low.min())

                #print(gap_revisit_price,item[1])

                # if gap has been revisited to 70% in length from the top, ignore the gap, it is considered filled
                # gap_top = item[1]
                if (gap_revisit_price > 0.7 * item[1]):


                    # if some of the gap has been revisited , replace the top with revisted price
                    if gap_revisit_price < item[1]:
                        gap_top = gap_revisit_price
                    else:
                        gap_top = item[1]

                    gap_bottom = item[6]
                    
                    #print(idx)
                    #item[0] = original gap up date, leave it as it is 
                    gaps[ticker + str(idx)] = {'gap_date':item[0],'Gap_Top': gap_top,'Gap_Bottom':gap_bottom}

                    print(gaps)

        gap_stocks.append(gaps)

    
    for ticker in tickers:
        try:
            ret_gaps_stocks(ticker,start, now , gap_percent)
        except:
            pass
    
    print(gap_stocks)





    









    






