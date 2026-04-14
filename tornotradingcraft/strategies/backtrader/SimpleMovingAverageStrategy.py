import backtrader as bt

class SimpleMovingAverageStrategy(bt.Strategy):
    params = (
        ("maperiod", 15),
        ("exitbars", 5),
    )

    def log(self, txt, dt=None):
        """Logging function for this strategy"""
        dt = dt or self.datas[0].datetime.date(0)
        print("%s, %s" % (dt.isoformat(), txt))

    # def prenext(self):
    #     print("prenext:: current period:", len(self))

    # def nextstart(self):
    #     print("nextstart:: current period:", len(self))
    #     # emulate default behavior ... call next
    #     self.next()

    def __init__(self):
        # To keep track of pending orders and buy price/commission
        self.order = None
        self.buyprice = None
        self.buycomm = None
        self.bar_executed = len(self)
        self.movav = bt.indicators.MovingAverageSimple(
            self.datas[0], period=self.params.maperiod
        )
        self.order_history = []
        self.trade_history = []

        # Indicators for the plotting show
        # bt.indicators.ExponentialMovingAverage(self.datas[0], period=25)
        # bt.indicators.WeightedMovingAverage(self.datas[0], period=25,
        #                                     subplot=True)
        # bt.indicators.StochasticSlow(self.datas[0])
        # bt.indicators.MACDHisto(self.datas[0])
        # rsi = bt.indicators.RSI(self.datas[0])
        # bt.indicators.SmoothedMovingAverage(rsi, period=10)
        # bt.indicators.ATR(self.datas[0], plot=False)

    def notify_order(self, order):
        if order.status in [order.Submitted]:
            print("ORDER SUBMITTED")
            return
        elif order.status in [order.Accepted]:
            print("ORDER ACCEPTED")
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                print(
                    "BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f"
                    % (order.executed.price, order.executed.value, order.executed.comm)
                )
                self.order_history.append(order)
            elif order.issell():
                print(
                    "SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f"
                    % (order.executed.price, order.executed.value, order.executed.comm)
                )
                self.order_history.append(order)

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            print("Order Canceled/Margin/Rejected")

        # Write down: no pending order
        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.trade_history.append(trade)

        print("OPERATION PROFIT, GROSS %.2f, NET %.2f" % (trade.pnl, trade.pnlcomm))

    # def start(self):
    #     print("Backtesting is about to start")

    # def stop(self):
    #     print("Backtesting is finished")

    def next(self):
        # Simply log the closing price of the series from the reference
        print("next:: current period:", len(self))
        self.log(
            "Portfolio: %.2f, Cash: %0.2f, Open: %.2f, Close: %.2f"
            % (
                self.broker.getvalue(),
                self.broker.getcash(),
                self.datas[0].open[0],
                self.datas[0].close[0],
            )
        )

        # Print the current position
        # print(self.getposition())

        # Check if an order is pending ... if yes, we cannot send a 2nd one
        if self.order:
            return

        if not self.position:

            if self.datas[0].close[0] > self.movav.lines.sma[0]:

                # Keep track of the created order to avoid a 2nd order
                self.order = self.buy()

                # Log the buy order
                print("BUY CREATE, %.2f" % self.datas[0].close[0])

        else:

            if self.datas[0].close[0] > self.movav.lines.sma[0]:

                # Keep track of the created order to avoid a 2nd order
                self.order = self.sell()

                # Log the sell order
                print("SELL CREATE, %.2f" % self.datas[0].close[0])