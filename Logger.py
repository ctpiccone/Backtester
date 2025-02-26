from abc import ABCMeta, abstractmethod
import pandas as pd
import datetime

import Events


class Logger(metaclass=ABCMeta):
    pass


class PortfolioLogger(Logger):

    def __init__(self, portfolio):
        self.portfolio = portfolio
        self.log_port = pd.DataFrame(columns=["Time", "Realized P&L", "Unrealized P&L", "Value"])

    def log(self, time, daily_return):
        self.log_port = pd.concat([self.log_port, pd.DataFrame({"Time":[time], "Realized P&L":[self.portfolio.realized_PL], "Unrealized P&L":[self.portfolio.unrealized_PL], "Value": [self.portfolio.value()], })])

    def get_value(self, date: datetime):
        matching = self.log_port[self.log_port]


class FillLogger(Logger):

    def __init__(self):
        self.log_fill = pd.DataFrame(columns=["Time", "Symbol", "Quantity", "Cost", "Exchange"])

    def log(self, fill: Events.FillEvent):
        self.log_fill = pd.concat(
            [self.log_fill, pd.DataFrame([fill.datetime, fill.ticker, fill.quantity, fill.cost, fill.exchange])])


class TradeLogger(Logger):
    pass
