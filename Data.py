from abc import ABCMeta, abstractmethod
import pandas as pd
import os
import numpy as np
import datetime

import Events


class DataHandler(metaclass=ABCMeta):

    @abstractmethod
    def __init__(self, tickers=[]):
        self.queue = None
        self.tickers = tickers
        self.start_date = None
        self.end_date = None
        self.current_data = {}
        self.continue_backtest = True
        self.data = {}

    @abstractmethod
    def initialize(self):
        raise NotImplementedError("Should implement initialize()")

    @abstractmethod
    def get_latest(self, ticker, N=1):
        raise NotImplementedError("Should implement get_latest_bar()")

    @abstractmethod
    def get_latest_datetime(self, symbol):
        raise NotImplementedError("Should implement get_latest_bar_datetime()")

    @abstractmethod
    def update(self, dt: datetime.datetime, N=1):
        raise NotImplementedError("Should implement get_latest_bars_values")

    @abstractmethod
    def set_events(self, queue):
        raise NotImplementedError("Should implement set_events")

    @abstractmethod
    def set_dates(self, start_date, end_date=None):
        raise NotImplementedError("Should implement set_dates()")


class CSVDataHandler(DataHandler):

    def __init__(self, dir, tickers=[]):
        super().__init__(tickers)
        # specific to CSVDataHandler
        self.dir = dir

    def initialize(self):
        self.data = self.read_csv()
        # only initialize current data for a ticker if the data is "current"
        for ticker in self.tickers:
            if self.data[ticker].index[0] == self.start_date:
                self.current_data[ticker] = pd.DataFrame(self.data[ticker].iloc[0:1])

    def set_events(self, queue):
        self.queue = queue

    def set_dates(self, start_date, end_date=None):
        if type(start_date)==str:
            self.start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        else:
            self.start_date = start_date
        if type(end_date)==str:
            self.end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        else:
            self.end_date = end_date

    def read_csv(self):
        data = {}
        # if no selection take all files in directory
        if len(self.tickers) == 0:
            # make sure to only take csvs
            for file in [file for file in os.listdir(self.dir) if ".csv" in file]:
                ticker = file[:-4]
                self.tickers.append(ticker)
                data[ticker] = pd.read_csv(self.dir + "\\" + file, index_col=0, parse_dates=True)
        # pre-selected list of tickers
        else:
            for file in [ticker + ".csv" for ticker in self.tickers]:
                data[file[:-4]] = pd.read_csv(self.dir + "\\" + file, index_col=0, parse_dates=True)
        # ensure formatting of dates for all tickers is consistent with start and end dates
        min_date = min([data[k].index[0] for k in data.keys()])
        if self.start_date is not None:
            for ticker in data.keys():
                data[ticker] = data[ticker][data[ticker].index >= self.start_date]
        else:
            self.start_date = min_date
        if self.end_date is not None:
            for ticker in data.keys():
                data[ticker] = data[ticker][data[ticker].index <= self.end_date]
        return data

    def get_latest(self, ticker, N=1):
        return self.current_data[ticker].iloc[-N:]

    def get_latest_datetime(self, symbol):
        return self.current_data[symbol].index[-1]

    def update(self, dt, N: int = 1):
        new_date = None
        # convert to date to DateTime
        if type(dt) == str:
            dt = datetime.datetime.strptime(dt, "%Y-%m-%d")
        # update each ticker
        for ticker in self.tickers:
            if dt == self.end_date:
                self.continue_backtest = False
            #add in new symbol to current_data if date has been reached and it is not already in current_data
            if ticker not in self.current_data.keys():
                if self.data[ticker].index[0]==dt:
                    self.current_data[ticker] = self.data[ticker].loc[self.data[ticker].index==dt]
                else:
                    continue
            if self.get_latest_datetime(ticker) == dt:
                dt = self.data[ticker].index[list(self.data[ticker].index).index(dt)+1]
            new_data = self.data[ticker][self.get_latest_datetime(ticker) < self.data[ticker].index]
            new_data = new_data[new_data.index <= dt]
            new_date = new_data.index[0]
            self.current_data[ticker] = pd.concat([self.current_data[ticker], new_data])
        self.queue.put(Events.MarketEvent())
        return new_date


class AlpacaDataHandler(DataHandler):

    def initialize(self):
        pass

    def get_latest(self, ticker, N=1):
        pass

    def get_latest_datetime(self, symbol):
        pass

    def update(self, dt, N=1):
        pass

    def set_events(self, queue):
        pass

    def set_dates(self, start_date, end_date=None):
        if type(start_date)==str:
            self.start_date=datetime.datetime.strptime(start_date, "%Y-%m-%d")
        if end_date is None:
            self.end_date = datetime.datetime.today()
        if type(end_date)==str:
            self.end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
