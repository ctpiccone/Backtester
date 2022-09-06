from abc import ABCMeta, abstractmethod
import pandas as pd
import os
import numpy as np
import datetime
from Data import DataHandler
from alpaca.data import StockHistoricalDataClient
import alpaca.data.requests as rq
import Events

API_KEY = "PK2TB7B916DFYNRU55MC"
SECRET_KEY = "DArxz2z8dG6B5kL0Oi0rXWIWd6i1F7ARydjWLNTE"


class AlpacaDataHandler(DataHandler):

    def set_events(self, queue):
        self.queue = queue

    def set_dates(self, start_date, end_date=None):
        if type(start_date) == str:
            self.start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        else:
            self.start_date = start_date
        if end_date is None:
            self.end_date = datetime.date.today()
        elif type(end_date) == str:
            self.end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        else:
            self.end_date = end_date


class TradesAlpacaDataHandler(AlpacaDataHandler):

    def __init__(self, tickers=[]):
        super().__init__(tickers)
        self.client = StockHistoricalDataClient(API_KEY, SECRET_KEY)
        for ticker in self.tickers:
            self.data[ticker] = None
            self.current_data[ticker] = None

    def initialize(self):
        # only load in first 10,000 points of data to prevent too much memory usage
        request = rq.StockTradesRequest(symbol_or_symbols=self.tickers, start=self.start_date, limit=100000)
        response = self.client.get_stock_trades(request).data
        # populate self.data
        for ticker in response.keys():
            # format response into proper dataframe
            nested_response = [[x.timestamp, x.exchange, x.price, x.size, x.conditions, x.tape, x.id] for x in response[ticker]]
            response_dt = [x[0] for x in nested_response]
            self.data[ticker] = pd.DataFrame(nested_response, index=response_dt, columns=["Exchange", "Price", "Size", "Conditions", "Tape", "ID"])

    def update(self, dt, N=1):
        for ticker in self.current_data.keys():
            # If there is no data to update it with, skip iteration
            if self.data[ticker] is None:
                continue
            #check to make sure there is still new data in self.data
            if len([x for x in self.data[ticker].index if x>self.get_latest_datetime(ticker)])==0:
                prev_date = self.get_latest_datetime(ticker)
                self.data[ticker] = self.__get_data(ticker, prev_date)
            # based on dt, if there is no data before it in self.data, get the next datapoint
            if len([x for x in self.data.index if x<dt])==0:
                # need to find index that is closest to dt
                target_index = min([x-dt for x in self.data[ticker].index if x > dt])
                data = self.data[ticker].iloc[target_index]
                self.__append(ticker, data)
            # if there is data before dt, grab all of it and do not get next datapoint
            else:
                target_index = min([x - dt for x in self.data[ticker].index if x not in self.current_data[ticker]])
                data = self.data[ticker].iloc[target_index]
                self.__append(ticker, data)

    def get_latest(self, ticker, N=1):
        return self.current_data[ticker].iloc[self.current_data[ticker].index[len(self.current_data[ticker].index)-N]:self.current_data[ticker].index[len(self.current_data[ticker].index)-1]-1]

    def get_latest_datetime(self, symbol):
        pass

    def __append(self, ticker, data):
        self.current_data[ticker].loc[len(self.current_data[ticker].index)] = data

    def __get_data(self, ticker, dt):
        request = rq.StockTradesRequest(symbol_or_symbols=ticker, start=dt, limit=100000)
        response = self.client.get_stock_trades(request).data[ticker]
        nested_response = [[x.timestamp, x.exchange, x.price, x.size, x.conditions, x.tape, x.id] for x in response]
        response_dt = [x[0] for x in nested_response]
        return pd.DataFrame(nested_response, index=response_dt, columns=["Exchange", "Price", "Size", "Conditions", "Tape", "ID"])




