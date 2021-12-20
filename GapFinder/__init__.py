import datetime as dt
import logging
import requests
from decimal import Decimal
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

    # set it manually 
    max_files = 14
    
    account_name = 'mkccsvs'
    account_Key = 'N33wcyz+gndJj/laNFLy4mdG7yhE+5TTkuBD9DCCnXN5F4v/bAz71hrn2+UZNtvIsx1nkS2VPw8rJI/IZnfBmA=='

    table_service = TableService(account_name=account_name, account_key=account_Key)

    entity = table_service.get_entity('gapstocks1', 'whichgap', '1')

    read_from = entity['readfrom']

    print(read_from)

    if(read_from != 0):
        startidx = 200 * read_from + 1
    else:
        startidx = read_from

    endidx = startidx + 199
    

    gap_stocks = []

    start = dt.datetime.now() + dt.timedelta(days= - 50)

    now = dt.datetime.now()

    gap_percent = 4

    column_name = ['Ticker']

    list_tickerurl = [
    'https://mkccsvs.blob.core.windows.net/shortlist/SNP.csv?sp=r&st=2021-11-22T00:00:00Z&se=2023-02-01T04:28:51Z&sv=2020-08-04&sr=b&sig=TNZG8Ps0bHV9Nf802eXc796UrV0gyjJEQOph6qHGPO4%3D',
    'https://mkccsvs.blob.core.windows.net/shortlist/SNP2.csv?sp=racwd&st=2021-11-25T05:04:36Z&se=2023-02-02T13:04:36Z&sv=2020-08-04&sr=b&sig=MoYeFEmod0tPLjAXl2hkC1IK2FVDCRsB27MCHH4Hkg8%3D'
    ]

    tickerdf = pd.read_csv('https://mkccsvs.blob.core.windows.net/shortlist/tickerlist.csv?sp=r&st=2021-12-19T00:52:22Z&se=2023-07-01T07:52:22Z&sv=2020-08-04&sr=b&sig=r5j6QOUlc2DIADsh4qiUHQzYjLwqHT1eWWGcqxArhmE%3D',names=column_name)

    tickers = tickerdf.Ticker.to_list()[startidx:endidx]

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
        
        #Dict of gaps
        
        #item is tuple from list of tuples
        for idx, item in enumerate(records):
            
            #print(item)
            # check if the current price is above the gapped up Top price 
            #'Gap_Top':item[1],'Gap_Bottom':item[6]

            if item[1] < current_price:

                #check if the gap has been filled earlier
                #check if open, close high or low has visted the gap range after gap has formed


                data = data[data.index > item[0]]

                gap_revisit_price =  min(data.Close.min(),data.Open.min(),data.High.min(),data.Low.min())

                #print(gap_revisit_price,item[1])

                # if gap has been revisited to 30% in length from the top, ignore the gap, it is considered filled
                # gap_top = item[1]

                gap_width = (item[1] - item[6])


                if (gap_revisit_price > (item[1] - 0.3 * gap_width )):


                    # if some of the gap has been revisited , replace the top with revisted price
                    if gap_revisit_price < item[1]:
                        gap_top = gap_revisit_price
                    else:
                        gap_top = item[1]

                    gap_bottom = item[6]
                    
                    #print(idx)
                    #item[0] = original gap up date, leave it as it is 

                    #gaps[ticker + str(idx)] = {'gap_date':item[0],'Gap_Top': gap_top,'Gap_Bottom':gap_bottom}

                    #gap_entity = {'PartitionKey': 'gapStocks', 'RowKey': ticker + str(idx),'Gap_Date':str(item[0])[:10],
                    #            'Gap_Top': str(round(gap_top,2)),'Gap_Bottom':str(round(gap_bottom,2))}

                    #print(gap_entity)

                    gap_stocks.append([ticker + str(idx),str(item[0])[:10],str(round(gap_top,2)),str(round(gap_bottom,2))])


                    #table_service.insert_or_replace_entity('gapstocks1', gap_entity)
                    
                  

    for ticker in tickers:

        try:
            ret_gaps_stocks(ticker,start, now , gap_percent)
        except:
            pass
    
    gaplistdf = pd.DataFrame(gap_stocks,columns=['Ticker','Gap_Date','Gap_Top','Gap_Bottom'])

    print(gaplistdf)

    
    # Insert the dictionaries from the gap_Stocks list to Azure Storage table

    #for gap_stock in gap_stocks:

    #    try:
    #        table_service.insert_or_replace_entity('gapstocks1', gap_stock)
    #    except:
    #        pass
    
    #Cleanup storage table, delete gaps filled or are no more required

    blob_block = ContainerClient.from_connection_string(
        conn_str = 'DefaultEndpointsProtocol=https;AccountName=mkccsvs;AccountKey=N33wcyz+gndJj/laNFLy4mdG7yhE+5TTkuBD9DCCnXN5F4v/bAz71hrn2+UZNtvIsx1nkS2VPw8rJI/IZnfBmA==;EndpointSuffix=core.windows.net',
        container_name = 'gapstocks'
        )

        #detele shortlist.csv if exists
    blob_service_client = BlobServiceClient.from_connection_string('DefaultEndpointsProtocol=https;AccountName=mkccsvs;AccountKey=N33wcyz+gndJj/laNFLy4mdG7yhE+5TTkuBD9DCCnXN5F4v/bAz71hrn2+UZNtvIsx1nkS2VPw8rJI/IZnfBmA==;EndpointSuffix=core.windows.net')
    container_client = blob_service_client.get_container_client('gapstocks')

    output = StringIO()
    output = gaplistdf.to_csv(encoding='utf-8')
    
    outputfile = 'gaplist'  + str(read_from) + '.csv'
    
    try:
        container_client.delete_blob(outputfile)
    except:
        pass

    blob_block.upload_blob(outputfile, output, overwrite=True, encoding='utf-8')

    if read_from < max_files:
        read_from = read_from + 1
    else:
        read_from = 0
        
    table_service.insert_or_replace_entity('gapstocks1', {'PartitionKey': 'whichgap', 'RowKey': '1', 'readfrom' : read_from})



 








    





    









    






