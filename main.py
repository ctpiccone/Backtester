# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import os
import datetime
import Data
import queue
import Backtest
import Execution
import Strategy
import Portfolio
import AlpacaData


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    start_date = "2022-11-01"
    dh = AlpacaData.BarsAlpacaDataHandler(["AAPL", "KR"], "m")
    portfolio = Portfolio.Portfolio(dh, 100000)
    backtest = Backtest.Backtest(["AAPL", "KR"], 100000, datetime.timedelta(minutes=120), start_date, datetime.datetime.now(), Execution.DummyExecutionHandler(), Strategy.MovingAverageStrategy(dh), portfolio, dh)
    backtest.run_backtest()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
