from abc import ABCMeta, abstractmethod
import pandas as pd
import os
import numpy as np
import math

import Events
import Logger
import Portfolio
import Data
import Logger


class ExecutionHandler(metaclass=ABCMeta):

    def __init__(self):
        self.fill_log = Logger.FillLogger()

    def calculate_quantity(self, portfolio: Portfolio, signal: Events.SignalEvent):
        raise NotImplementedError("Should implement calculate_quantity()")

    @abstractmethod
    def execute_order(self, portfolio: Portfolio, signal: Events.SignalEvent, data: Data.DataHandler):
        raise NotImplementedError("Should implement execute_order()")


class DummyExecutionHandler(ExecutionHandler):

    def execute_order(self, portfolio: Portfolio, signal: Events.SignalEvent, events, data: Data.DataHandler):
        to_close = None
        try:
            if signal.direction == 1:
                # buying to close
                if portfolio.get_position(signal.ticker).quantity < 0:
                    to_close = "CLOSE"
                # buying to open
                else:
                    to_close = "OPEN"
            else:
                # selling to open
                if portfolio.get_position(signal.ticker).quantity <= 0:
                    to_close = "OPEN"
                # selling to close
                else:
                    to_close = "CLOSE"
                portfolio.events.put(Events.FillEvent(signal.ticker, data.get_latest_datetime(signal.ticker), "FAKE",
                                                      self.calculate_quantity(portfolio, signal,
                                                                              data.get_latest(signal.ticker)[
                                                                                  "Adj Close"].iloc[
                                                                                  0]), signal.direction, to_close,
                                                      data.get_latest(signal.ticker)["Adj Close"].iloc[0], 0))
        except Portfolio.PositionNotFoundException:
            portfolio.events.put(Events.FillEvent(signal.ticker, data.get_latest_datetime(signal.ticker), "FAKE",
                                                  self.calculate_quantity(portfolio, signal,
                                                                          data.get_latest(signal.ticker)[
                                                                              "Adj Close"].iloc[0]), signal.direction,
                                                  "OPEN", data.get_latest(signal.ticker)["Adj Close"].iloc[0], 0))

    def calculate_quantity(self, portfolio: Portfolio, signal: Events.SignalEvent, price):
        if signal.direction == -1:
            return portfolio.get_position(signal.ticker).quantity
        else:
            return math.floor(portfolio.current_cash / price * signal.strength / 10)
