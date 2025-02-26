from abc import ABCMeta, abstractmethod

import numpy as np

import Events
import Data


class Strategy(metaclass=ABCMeta):

    @abstractmethod
    def calculate_signals(self):
        raise NotImplementedError("Should implement calculate_signals()")

    @abstractmethod
    def set_events(self, queue):
        raise NotImplementedError("Should implement set_queue")

    @abstractmethod
    def set_symbols(self, tickers):
        raise NotImplementedError("Should implement set_symbols")


class MovingAverageStrategy(Strategy):

    def __init__(self, bars: Data.DataHandler, short_window=50, long_window=100, tickers=None):
        self.bars = bars
        self.symbols = tickers
        self.short_window = short_window
        self.long_window = long_window
        self.events = None
        self.signals = {}

    def set_events(self, queue):
        self.events = queue

    def set_symbols(self, tickers):
        self.symbols = tickers
        for symbol in self.symbols:
            self.signals[symbol] = 0

    def calculate_signals(self):
        for symbol in self.bars.current_data.keys():
            if self.bars.current_data[symbol] is None:
                continue
            dt = self.bars.get_latest_datetime(symbol)
            long = self.bars.get_latest(symbol, self.long_window)
            short = self.bars.get_latest(symbol, self.short_window)
            if np.mean(short["Open"]) > np.mean(long["Open"]) and self.signals[symbol] != 1:
                # enter trade
                self.events.put(Events.SignalEvent(symbol, dt, 1.0, 1))
                self.signals[symbol] = 1
            elif np.mean(short["Open"]) < np.mean(long["Open"]) and self.signals[symbol] == 1:
                self.events.put(Events.SignalEvent(symbol, dt, 1.0, -1))
                self.signals[symbol] = 0
