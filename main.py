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

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # files = os.listdir(r"C:\Users\cameron.Piccone\Documents\TestData")
    # print(files)
    dh = Data.CSVDataHandler(r"C:\Users\cameron.Piccone\\Documents\TestData")
    # dh.initialize()
    # dh.set_events(queue.Queue())
    # dh.update(dh.start_date+datetime.timedelta(days=1))
    # print()
    portfolio = Portfolio.Portfolio(dh, 100000)
    backtest = Backtest.Backtest(["AAPL", "KR"], 100000, 0, "2021-07-28", "2022-06-01", Execution.DummyExecutionHandler(), Strategy.MovingAverageStrategy(dh), portfolio, dh)
    backtest.run_backtest()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
