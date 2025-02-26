from abc import ABCMeta, abstractmethod
import pandas as pd
import os
import numpy as np
import datetime
from Data import DataHandler
from alpaca.data import StockHistoricalDataClient
import alpaca.data.requests as rq
import Events
from alpaca.data.timeframe import TimeFrame

class NoSuchIntervalAvailable(Exception):

    def __init__(self, interval):
        allowed = ["d", "h", "m"]
        self.message = f"Bar interval {interval} is not available. Parameter must be one of the following:{allowed}"
        super().__init__(self.message)

# TODO add market events functionality
# TODO add test to confirm events
# TODO add continue_backtest functionality
# TODO test continue_backtest functionality
# TODO at the end upload to github

API_KEY = "PK2TB7B916DFYNRU55MC"
SECRET_KEY = "DArxz2z8dG6B5kL0Oi0rXWIWd6i1F7ARydjWLNTE"


class AlpacaDataHandler(DataHandler):

    def __init__(self, tickers=[]):
        super().__init__(tickers)
        self.client = StockHistoricalDataClient(API_KEY, SECRET_KEY)
        for ticker in self.tickers:
            self.data[ticker] = None
            self.current_data[ticker] = None
            self.has_more_data[ticker] = True

    def set_events(self, queue):
        self.queue = queue

    def set_dates(self, start_date, end_date=None):
        if type(start_date) == str:
            self.start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        else:
            self.start_date = start_date
        if end_date is None:
            self.end_date = datetime.datetime.now()
        elif type(end_date) == str:
            self.end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        else:
            self.end_date = end_date


class BarsAlpacaDataHandler(AlpacaDataHandler):
    OFFSET = datetime.timedelta(days=60)

    def __init__(self, ticker, timeframe):
        super().__init__(ticker)
        if timeframe == "d":
            self.time_frame = TimeFrame.Day
        elif timeframe == "h":
            self.time_frame = TimeFrame.Hour
        elif timeframe == "m":
            self.time_frame = TimeFrame.Minute
        else:
            raise NoSuchIntervalAvailable(timeframe)

    def initialize(self):
        # only load in 4 hours increments
        intermidiate_end = self.start_date + BarsAlpacaDataHandler.OFFSET
        if intermidiate_end > self.end_date:
            intermidiate_end = self.end_date
        request = rq.StockBarsRequest(symbol_or_symbols=self.tickers, start=self.start_date, end=intermidiate_end,
                                      adjustment="all", timeframe=self.time_frame)
        response = self.client.get_stock_bars(request).data
        # populate self.data
        for ticker in response.keys():
            # format response into proper dataframe
            nested_response = [[x.open, x.high, x.low, x.close, x.vwap, x.volume, x.trade_count] for x in
                               response[ticker]]
            response_dt = [x.timestamp.replace(tzinfo=datetime.timezone.utc) for x in response[ticker]]
            self.data[ticker] = pd.DataFrame(nested_response, index=response_dt,
                                             columns=["Open", "High", "Low", "Close", "Volume-Weighted_Average_Price",
                                                      "Volume", "Trades"])

    def update(self, dt, N=1):
        dt = dt.replace(tzinfo=datetime.timezone.utc)
        next_dates = []
        for ticker in self.current_data.keys():
            # If there is no data to update it with, skip iteration
            if self.data[ticker] is None:
                continue
            # we want everything before dt in data
            data = self.data[ticker][self.data[ticker].index <= dt]
            # exclude data already in current_data
            data = data[data.index > self.get_latest_datetime(ticker)]
            self.__append(ticker, data)
            # check to make sure there is still new data in self.data
            # If not, update self.data
            if self.data[ticker].index[-1] <= self.get_latest_datetime(ticker):
                prev_date = self.get_latest_datetime(ticker)
                self.data[ticker] = self.__get_data(ticker, prev_date)
            # return next available dt in self.data
            if self.current_data[ticker] is None:
                next_dates.append(self.data[ticker].index[0])
            # if there is no more data to update
            elif self.data[ticker].index[len(self.data[ticker].index)-1]<=dt  and data.index[-1]==self.data[ticker].index[-1]:
                self.has_more_data[ticker]=False
                self.__append(ticker,data)
            else:
                target_index = list(self.data[ticker].index).index(self.get_latest_datetime(ticker))
                next_dates.append(self.data[ticker].index[target_index + 1])
        self.queue.put(Events.MarketEvent())
        if True not in [self.has_more_data[x] for x in self.has_more_data.keys()]:
            # no more data to update for any tickers
            self.continue_backtest=False
            return
        return min(next_dates)

    def get_latest(self, symbol, N=1):
        if N >= len(self.current_data[symbol].index):
            return self.current_data[symbol]
        elif N == 1:
            return self.current_data[symbol].iloc[[len(self.current_data[symbol].index) - 1]]
        else:
            return self.current_data[symbol].iloc[
                   len(self.current_data[symbol].index) - N:len(self.current_data[symbol].index)]

    def get_latest_datetime(self, symbol, N=1):
        if self.current_data[symbol] is None:
            return self.start_date.replace(tzinfo=datetime.timezone.utc) - datetime.timedelta(microseconds=500)
        else:
            if len(self.get_latest(symbol, N).index) == 1:
                return self.get_latest(symbol, N).index[0]
            else:
                return self.get_latest(symbol, N).index

    def __append(self, ticker, data):
        if data.empty:
            return
        if self.current_data[ticker] is None:
            self.current_data[ticker] = data
        else:
            self.current_data[ticker] = self.current_data[ticker].append(data)

    def __get_data(self, ticker, dt):
        # only load in 4 hours increments
        intermidiate_end = dt + BarsAlpacaDataHandler.OFFSET
        if intermidiate_end.replace(tzinfo=datetime.timezone.utc) > self.end_date.replace(tzinfo=datetime.timezone.utc):
            intermidiate_end = self.end_date
        request = rq.StockBarsRequest(symbol_or_symbols=self.tickers, start=dt.replace(tzinfo=None), end=intermidiate_end.replace(tzinfo=None),
                                      adjustment="all", timeframe=self.time_frame)
        response = self.client.get_stock_bars(request).data
        nested_response = [[x.open, x.high, x.low, x.close, x.vwap, x.volume, x.trade_count] for x in response[ticker]]
        response_dt = [x.timestamp.replace(tzinfo=datetime.timezone.utc) for x in response[ticker]]
        return pd.DataFrame(nested_response, index=response_dt,
                            columns=["Open", "High", "Low", "Close", "Volume-Weighted_Average_Price", "Volume",
                                     "Trades"])


class QuotesAlpacaDataHandler(AlpacaDataHandler):
    OFFSET = datetime.timedelta(hours=1)

    def initialize(self):
        # only load in 4 hours increments
        intermidiate_end = self.start_date + QuotesAlpacaDataHandler.OFFSET
        if intermidiate_end > self.end_date:
            intermidiate_end = self.end_date
        request = rq.StockQuotesRequest(symbol_or_symbols=self.tickers, start=self.start_date, end=intermidiate_end)
        response = self.client.get_stock_quotes(request).data
        # populate self.data
        for ticker in response.keys():
            # format response into proper dataframe
            nested_response = [
                [x.ask_exchange, x.ask_price, x.ask_size, x.bid_exchange, x.bid_price, x.bid_size, x.tape, x.conditions]
                for x in response[ticker]]
            response_dt = [x.timestamp.replace(tzinfo=datetime.timezone.utc) for x in response[ticker]]
            self.data[ticker] = pd.DataFrame(nested_response, index=response_dt,
                                             columns=["ask_exchange", "ask_price", "ask_quantity", "bid_exchange",
                                                      "bid_price", "bid_quantity", "tape", "conditions"])

    def update(self, dt, N=1):
        dt = dt.replace(tzinfo=datetime.timezone.utc)
        next_dates = []
        for ticker in self.current_data.keys():
            # If there is no data to update it with, skip iteration
            if self.data[ticker] is None:
                continue
            # we want everything before dt in data
            data = self.data[ticker][self.data[ticker].index <= dt]
            # exclude data already in current_data
            data = data[data.index > self.get_latest_datetime(ticker)]
            self.__append(ticker, data)
            # check if there is more data in self.data, if not, get more data
            if not [x for x in list(self.data[ticker].index) if x > self.get_latest_datetime(ticker)]:
                prev_date = self.get_latest_datetime(ticker)
                self.data[ticker] = self.__get_data(ticker, prev_date)
                next_dates.append(self.data[ticker].index[0])
            # return next available dt in self.data
            elif self.current_data[ticker] is None:
                next_dates.append(self.data[ticker].index[0])
            # case where current data is populated but we have no new data
            else:
                next_dates.append([x for x in list(self.data[ticker].index) if x > dt][0])
        self.queue.put(Events.MarketEvent())
        return min(next_dates)

    # For quotes, N refers to last N Quotes by time, not last N quotes
    def get_latest(self, symbol, N=1):
        if N >= len(self.current_data[symbol].index):
            return self.current_data[symbol]
        elif N == 1 or len(set(self.current_data[symbol].index)) == 1:
            target_dates = list(self.current_data[symbol].index)[-1]
            bool_arr = [x == target_dates for x in self.current_data[symbol].index]
            return self.current_data[symbol][bool_arr]
        else:
            target_dates = []
            [target_dates.append(x) for x in self.current_data[symbol].index if x not in target_dates]
            bool_arr = [x in target_dates[-1 - N] for x in self.current_data[symbol].index]
            return self.current_data[symbol][self.current_data[symbol].index in target_dates]

    def get_latest_datetime(self, symbol, N=1):
        if self.current_data[symbol] is None:
            return self.start_date.replace(tzinfo=datetime.timezone.utc) - datetime.timedelta(microseconds=500)
        else:
            if N == 1:
                return self.current_data[symbol].index[len(self.current_data[symbol].index) - 1]
            else:
                dates = []
                [dates.append(x) for x in self.get_latest(symbol, N).index if x not in dates]
                return dates if len(dates) > 1 else dates[0]

    def __append(self, ticker, data):
        if data.empty:
            return
        if self.current_data[ticker] is None:
            self.current_data[ticker] = data
        else:
            self.current_data[ticker] = self.current_data[ticker].append(data)

    def __get_data(self, ticker, dt):
        intermidiate_end = dt + QuotesAlpacaDataHandler.OFFSET
        if intermidiate_end > self.end_date.replace(tzinfo=datetime.timezone.utc):
            intermidiate_end = self.end_date
        request = rq.StockQuotesRequest(symbol_or_symbols=ticker,
                                        start=dt.replace(tzinfo=None) + datetime.timedelta(microseconds=1),
                                        end=intermidiate_end.replace(tzinfo=None))
        response = self.client.get_stock_quotes(request).data[ticker]
        nested_response = [
            [x.ask_exchange, x.ask_price, x.ask_size, x.bid_exchange, x.bid_price, x.bid_size, x.tape,
             x.conditions] for x in response]
        response_dt = [x.timestamp.replace(tzinfo=datetime.timezone.utc) for x in response]
        return pd.DataFrame(nested_response, index=response_dt,
                            columns=["ask_exchange", "ask_price", "ask_quantity", "bid_exchange",
                                     "bid_price", "bid_quantity", "tape", "conditions"])


class TradesAlpacaDataHandler(AlpacaDataHandler):
    OFFSET = datetime.timedelta(hours=1)

    def initialize(self):
        # only load in 4 hours increments
        intermidiate_end = self.start_date + TradesAlpacaDataHandler.OFFSET
        if intermidiate_end > self.end_date:
            intermidiate_end = self.end_date
        request = rq.StockTradesRequest(symbol_or_symbols=self.tickers, start=self.start_date, end=intermidiate_end)
        response = self.client.get_stock_trades(request).data
        # populate self.data
        for ticker in response.keys():
            # format response into proper dataframe
            nested_response = [[x.exchange, x.price, x.size, x.conditions, x.tape, x.id] for x in response[ticker]]
            response_dt = [x.timestamp.replace(tzinfo=datetime.timezone.utc) for x in response[ticker]]
            self.data[ticker] = pd.DataFrame(nested_response, index=response_dt,
                                             columns=["Exchange", "Price", "Size", "Conditions", "Tape", "ID"])

    def update(self, dt, N=1):
        dt = dt.replace(tzinfo=datetime.timezone.utc)
        next_dates = []
        for ticker in self.current_data.keys():
            # If there is no data to update it with, skip iteration
            if self.data[ticker] is None:
                continue

            # we want everything before dt in data
            data = self.data[ticker][self.data[ticker].index <= dt]
            # exclude data already in current_data
            data = data[data.index > self.get_latest_datetime(ticker)]
            self.__append(ticker, data)

            # check to see if there is still new data in self.data
            # If not, update self.data
            if not [x for x in list(self.data[ticker].index) if x > self.get_latest_datetime(ticker)]:
                prev_date = self.current_data[ticker].index[-1]
                self.data[ticker] = self.__get_data(ticker, prev_date)
                next_dates.append(self.data[ticker].index[0])
            # return next available dt in self.data
            elif self.current_data[ticker] is None:
                next_dates.append(self.data[ticker].index[0])
            # case where current data is populated and there is more in data to update with
            else:
                next_dates.append([x for x in list(self.data[ticker].index) if x > dt][0])

            # return next available dt in self.data
            if self.current_data[ticker] is None:
                next_dates.append(self.data[ticker].index[0])
            # case where current data is populated but we have no new data
            else:
                target_index = list(self.data[ticker].index).index(self.get_latest_datetime(ticker))
                next_dates.append(self.data[ticker].index[target_index + 1])
        self.queue.put(Events.MarketEvent())
        return min(next_dates)

    def get_latest(self, symbol, N=1):
        if N >= len(self.current_data[symbol].index):
            return self.current_data[symbol]
        elif N == 1:
            return self.current_data[symbol].iloc[[len(self.current_data[symbol].index) - 1]]
        else:
            return self.current_data[symbol].iloc[
                   len(self.current_data[symbol].index) - N:len(self.current_data[symbol].index)]

    def get_latest_datetime(self, symbol, N=1):
        if self.current_data[symbol] is None:
            return self.start_date.replace(tzinfo=datetime.timezone.utc) - datetime.timedelta(microseconds=500)
        else:
            if len(self.get_latest(symbol, N).index) == 1:
                return self.get_latest(symbol, N).index[0]
            else:
                return self.get_latest(symbol, N).index

    def __append(self, ticker, data):
        if data.empty:
            return
        if self.current_data[ticker] is None:
            self.current_data[ticker] = data
        else:
            self.current_data[ticker] = self.current_data[ticker].append(data)

    def __get_data(self, ticker, dt):
        intermidiate_end = dt + TradesAlpacaDataHandler.OFFSET
        if intermidiate_end.replace(tzinfo=datetime.timezone.utc) > self.end_date.replace(tzinfo=datetime.timezone.utc):
            intermidiate_end = self.end_date
        request = rq.StockTradesRequest(symbol_or_symbols=ticker, start=dt.replace(tzinfo=None), end=intermidiate_end.replace(tzinfo=None))
        response = self.client.get_stock_trades(request).data[ticker]
        nested_response = [[x.exchange, x.price, x.size, x.conditions, x.tape, x.id] for x in response]
        response_dt = [x.timestamp.replace(tzinfo=datetime.timezone.utc) for x in response]
        return pd.DataFrame(nested_response, index=response_dt,
                                         columns=["Exchange", "Price", "Size", "Conditions", "Tape", "ID"])