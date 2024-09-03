import pandas as pd
import backtrader as bt

class CustomPandasData(bt.feeds.PandasData):
    lines = ('delta',)  # Add the custom line 'delta'

    # Define the parameters
    params = (
        ('delta', -1),  # Default position for the custom line
    )

class MyStrategy(bt.Strategy):
    def __init__(self):
        self.price = self.datas[0].close
        self.delta = self.datas[0].delta
        print(list(self.price), list(self.delta))
        self.results = []

    def next(self):
        current_position_size = self.position.size
        deltaToHedge = self.delta[0] - current_position_size * self.price[0]
        if deltaToHedge > 0:
            self.sell(size=deltaToHedge)
            self.results.append({
                    'time': self.datas[0].datetime.datetime(0),
                    'action': 'buy',
                    'price':  self.price[0]
                })
        elif deltaToHedge < 0:
            self.buy(size=deltaToHedge)
            self.results.append({
                    'time': self.datas[0].datetime.datetime(0),
                    'action': 'buy',
                    'price':  self.price[0]
                })

def runBacktest(asset: str, data: pd.DataFrame):

    print(f'Running for {asset}')
    data['delta'] = data[f'{asset}AUMDelta'] + data[f'{asset}OIDiff']
    data['close'] = data[f'CEX{asset}Price']
    data_feed = CustomPandasData(
        dataname=data,
        datetime=None,  
        open=None,
        high=None,
        low=None,
        close='close',
        volume=None,
        openinterest=None,
        delta='delta'  # Custom line
    )

    cerebro = bt.Cerebro()
    cerebro.adddata(data_feed)
    cerebro.addstrategy(MyStrategy)
    cerebro.run()

    # Extract results from strategy
    strategy = cerebro.runstrats[0][0]
    print(strategy.results)