import datetime
import queue

import pandas as pd
import matplotlib.pyplot as plt
import Performance


class Backtest:
    def __init__(self, tickers, initial_capital, heartbeat, start_date, end_date, execution_handler,
                 strategy, portfolio, *data_handlers, warmup=0):
        self.tickers = tickers
        self.initial_capital = initial_capital
        self.heartbeat = heartbeat
        self.start_date = start_date
        self.end_date = end_date
        self.execution_handler = execution_handler
        self.portfolio = portfolio
        self.strategy = strategy
        self.warmup = warmup
        self.data_handlers = data_handlers
        for dh in self.data_handlers:
            dh.set_dates(start_date, end_date)
        self.events = queue.Queue()

    def backtest_stats(self):
        print("Portfolio Statistics")
        print(f"Final Value: {self.portfolio.value()}")
        print(f"Sharpe: {Performance.sharpe_ratio(self.portfolio.portfolio_logger.log_port)}")
        print(f"Probabilistic Sharpe: {Performance.prob_sharpe(self.portfolio.portfolio_logger.log_port)}")
        print(f"Max Drawdown: {Performance.max_drawdown(self.portfolio.portfolio_logger.log_port)}")

    def graph(self):
        y = self.portfolio.portfolio_logger.log_port["Value"]
        x = pd.to_datetime(self.portfolio.portfolio_logger.log_port["Time"])
        # get benchmark spy returns
        # get data
        bench = pd.read_csv(r"C:\Users\cameron.Piccone\Documents\TestData\Benchmarks\SPY.csv", parse_dates=["Date"])
        # synch dates with backtest
        bench = bench.loc[bench["Date"] >= self.portfolio.portfolio_logger.log_port["Time"].iloc[0]]
        bench = bench.loc[bench["Date"] <= self.portfolio.portfolio_logger.log_port["Time"].iloc[-1]]
        plt.plot(x, y, label="Porfolio")
        plt.plot(x, bench["Adj Close"] * (self.initial_capital / bench["Adj Close"].iloc[0]), label="Benchmark")
        plt.legend(loc="upper left")
        plt.xlabel("Date")
        plt.ylabel("Value")
        plt.show()

    def run_backtest(self):
        # initialize components
        cur_datetime = self.start_date
        print("Initializing backtest...")
        self.strategy.set_events(self.events)
        self.portfolio.set_events(self.events)
        for dh in self.data_handlers:
            dh.initialize()
            dh.set_events(self.events)
        self.strategy.set_symbols(self.tickers)
        if type(cur_datetime) == str:
            cur_datetime = datetime.datetime.strptime(cur_datetime, "%Y-%m-%d")
        # run backtest
        while True:
            # check if no more data is remaining
            if True not in [dh.continue_backtest for dh in self.data_handlers]:
                self.portfolio.liquidate()
                # edge to ensure that when we liquidate the fill is properly updated
                try:
                    event = self.events.get(False)
                except queue.Empty:
                    break
                self.portfolio.update_fill(event)
                event.print_fill()
                break
            inter_datetime = []
            # update all data handlers up to the current date
            for dh in self.data_handlers:
                inter_datetime.append(dh.update(cur_datetime))
            # handle all events in queue
            while True:
                try:
                    event = self.events.get(False)
                except queue.Empty:
                    break
                if event.type == "MARKET":
                    # there can be many at once, although all are from the same time
                    # market event means there is new data, therefore signals should recalculate
                    self.strategy.calculate_signals()
                    self.portfolio.update(cur_datetime)
                # place an order
                elif event.type == "SIGNAL":
                    self.portfolio.generate_order(event)
                # update portfolio stats with fill info
                elif event.type == "FILL":
                    self.portfolio.update_fill(event)
                    event.print_fill()
            # inter_datetime has the next available datetimes for reach data handler, we want the min so we update accordingly next loop
            cur_datetime = min(inter_datetime)
        self.backtest_stats()
        self.graph()
