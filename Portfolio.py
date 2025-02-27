import pandas as pd

import Events
import Execution
import Data
import datetime
import pandas
import Logger

class PositionNotFoundException(Exception):
    pass

class Position:

    def __init__(self, symbol, init_price, quantity, action):
        self.symbol = symbol
        self.init_price = init_price
        self.price = init_price
        self.action = action
        self.quantity = quantity
        self.cost_basis = init_price
        self.unrealized_pl = 0
        self.realized_pl = 0

    def update_price(self, close):
        # update price
        self.price = close
        self.unrealized_pl = self.value() - self.cost_basis * self.quantity

    def update_fill(self, fill: Events.FillEvent):
        prev_quant = self.quantity
        self.quantity += fill.quantity * fill.direction
        if fill.direction == 1:
            # no need to adjust realized p&l unless we were short before
            if prev_quant < 0:
                # covering a short
                self.realized_pl = self.realized_pl + (self.cost_basis - fill.cost) * fill.quantity
            else:
                # buying more
                self.cost_basis = (prev_quant * self.cost_basis + fill.quantity * fill.cost) / (
                        prev_quant + fill.quantity)
        else:
            if prev_quant > 0:
                # need to adjust p&l
                # selling position
                self.realized_pl += (fill.cost - self.cost_basis) * fill.quantity
            else:
                # adding to short
                self.cost_basis = (prev_quant * self.cost_basis + fill.quantity * fill.cost) / (
                        prev_quant + fill.quantity)
        # update unrealized pl
        self.unrealized_pl = self.value() - self.cost_basis * self.quantity

    def value(self):
        return self.price * self.quantity


class Portfolio:

    def __init__(self, data: Data.DataHandler, initial_capital):
        self.data = data
        self.initial_capital = initial_capital
        self.current_cash = initial_capital
        self.buying_power = initial_capital
        self.events = None
        self.current_positions = []
        self.portfolio_logger: Logger.PortfolioLogger = Logger.PortfolioLogger(self)
        self.fill_logger = Logger.FillLogger()
        self.realized_PL = 0
        self.unrealized_PL = 0
        self.cur_date = datetime.datetime(1900, 1, 1)


    def set_events(self, events):
        self.events = events

    def calculate_PL(self):
        days = sorted(list(set([x.date() for x in list(self.portfolio_logger.log_port.Time)])))
        self.portfolio_logger.log_port['Date'] = [x.date() for x in list(self.portfolio_logger.log_port.Time)]
        day_df = self.portfolio_logger.log_port[self.portfolio_logger.log_port.Date == days[0]]
        # formula is end of today minus end of yesterday
        ret = (day_df.iloc[-1].Value - day_df.iloc[0].Value) / day_df.iloc[0].Value
        pl_df = pd.DataFrame(
            {"Time": [x for x in list(self.portfolio_logger.log_port.Time) if x.date() == days[0]], "P_L": ret})
        for d in days[1:]:
            day_df = self.portfolio_logger.log_port[self.portfolio_logger.log_port.Date==d]
            # formula is end of today minus end of yesterday
            ret = (day_df.iloc[-1].Value - day_df.iloc[0].Value)/day_df.iloc[0].Value
            tmp = pd.DataFrame(
            {"Time": [x for x in list(self.portfolio_logger.log_port.Time) if x.date() == d], "P_L": ret})
            pl_df = pl_df.append(tmp)
        self.portfolio_logger.log_port["Return"] = list(pl_df["P_L"])
        self.portfolio_logger.log_port.pop("Date")
        print()


    def update(self, dt: datetime.datetime):
        for position in self.current_positions:
            position.update_price(self.data.get_latest(position.symbol)["Close"].iloc[0])
        self.unrealized_PL = sum([position.unrealized_pl for position in self.current_positions])
        # update buying power and cash_in_account
        self.buying_power = sum([position.unrealized_pl for position in self.current_positions])
        self.portfolio_logger.log(dt, self.cur_date)
        # check if new day, if so, update daily return
        # if self.cur_date.weekday() != dt.weekday() and self.cur_date != datetime.datetime(1900, 1, 1):
        #     yesterday_df = self.portfolio_logger.log_port[self.portfolio_logger.log_port["Time"] == self.cur_date]
        #     if yesterday_df.iloc[0]["Value"]==0:
        #         daily_return=0
        #     else:
        #         daily_return = (self.value() - yesterday_df.iloc[0]["Value"]) / yesterday_df.iloc[0]["Value"]
        #     self.portfolio_logger.log_port = self.portfolio_logger.log_port.replace(
        #         {"Daily Return": {self.cur_date.date(): daily_return}})
        # self.cur_date = dt
        if self.portfolio_logger.log_port.iloc[0,4] == datetime.datetime(1900, 1, 1):
            self.portfolio_logger.log_port.iloc[0, 4] = 0


    def value(self):
        return sum([position.value() for position in self.current_positions]) + self.current_cash

    def liquidate(self):
        for pos in self.current_positions:
            direction = -1 if pos.quantity>0 else 1
            self.generate_order(Events.SignalEvent(pos.symbol, self.cur_date, 1.0, direction))

    def update_fill(self, fill: Events.FillEvent):
        # create position
        pos = None
        if fill.ticker not in [pos.symbol for pos in self.current_positions]:
            pos = Position(fill.ticker, fill.cost, fill.quantity, fill.direction)
            self.current_positions.append(pos)
        # update position
        else:
            pos = [pos for pos in self.current_positions if pos.symbol == fill.ticker][0]
            pos.update_fill(fill)
        # need to update cash and buying power
        self.buying_power = self.current_cash-self.value()
        self.buying_power = self.value() - sum([pos.cost_basis*pos.quantity for pos in self.current_positions])
        self.realized_PL += pos.realized_pl
        if fill.direction == 1:
            self.current_cash -= fill.quantity * fill.cost
        else:
            self.current_cash += fill.quantity * fill.cost
        self.unrealized_PL = sum([position.unrealized_pl for position in self.current_positions])
        if self.get_position(fill.ticker).quantity==0:
            self.current_positions.remove(self.get_position(fill.ticker))
        self.fill_logger.log(fill)

    def generate_order(self, signal: Events.SignalEvent):
        try:
            # update buying power
            if signal.direction == 1:
                # buying to close
                if self.get_position(signal.ticker).quantity < 0:
                    to_close = "CLOSE"
                # buying to open
                else:
                    to_close = "OPEN"
                    self.buying_power -= Execution.DummyExecutionHandler().calculate_quantity(self, signal, self.data.get_latest(signal.ticker))
            else:
                # selling to open
                if self.get_position(signal.ticker).quantity <= 0:
                    to_close = "OPEN"
                    self.buying_power -= Execution.DummyExecutionHandler().calculate_quantity(self, signal, self.data.get_latest(signal.ticker))
                # selling to close
                else:
                    to_close = "CLOSE"
        except PositionNotFoundException:
            if signal.direction==1:
                # buying to open
                self.buying_power -= Execution.DummyExecutionHandler().calculate_quantity(self, signal, self.data.get_latest(signal.ticker)["Close"].iloc[0])
            else:
                # selling to open
                # TODO figure out buying power when shorting
                pass
        Execution.DummyExecutionHandler().execute_order(self, signal, self.events, self.data)

    def get_position(self, ticker: str) -> Position:
        if ticker not in [pos.symbol for pos in self.current_positions]:
            raise PositionNotFoundException
        return [pos for pos in self.current_positions if pos.symbol == ticker][0]
