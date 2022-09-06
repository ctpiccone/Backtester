class Event:
    pass


class MarketEvent(Event):
    def __init__(self):
        self.type = "MARKET"


class SignalEvent(Event):
    def __init__(self, ticker, datetime, strength, direction):
        self.type = "SIGNAL"
        self.ticker = ticker
        self.date = datetime
        self.direction = direction
        self.strength = strength


class FillEvent(Event):
    def __init__(self, ticker, datetime, exchange, quantity, direction, to_close, cost, commision):
        self.type = "FILL"
        self.ticker = ticker
        self.datetime = datetime
        self.exchange = exchange
        self.quantity = quantity
        self.direction = direction
        self.cost = cost
        self.commision = commision
        self.to_close = to_close

    def print_fill(self):
        direction = "BUY" if self.direction == 1 else "SELL"
        print("[FILL] {datetime}: {direction} to {to_close} {quantity} {ticker} at {cost} on {exchange}".\
              format(datetime=self.datetime, direction=direction, to_close= self.to_close, quantity=self.quantity, ticker=self.ticker, cost=self.cost, exchange=self.exchange))


class OrderEvent(Event):
    def __init__(self, ticker, order_type, quantity, direction, price=None):
        self.type = "ORDER"
        self.ticker = ticker
        self.order_type = order_type
        self.price = price
        self.quantity = quantity
        self.direction = direction

    def print_order(self):
        print("[ORDER]: {order_type} order for {quantity} {ticker} at {price}".\
              format(order_type=self.order_type, quantity=self.quantity, ticker=self.ticker, price=self.price))
