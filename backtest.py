import pandas as pd
from load_data import load_data

class Strategy:
    def __init__(self, data):
        self.data = data

    def generate_signal(self, current_index):
        short_window = 50
        long_window = 200

        if current_index < long_window:
            return 0  

        short_sma = self.data['price'].iloc[current_index - short_window + 1 : current_index + 1].mean()
        long_sma = self.data['price'].iloc[current_index - long_window + 1 : current_index + 1].mean()

        if short_sma > long_sma:
            return 1  
        elif short_sma < long_sma:
            return -1  
        else:
            return 0  

class Backtest:
    def __init__(self, data, strategy, initial_capital=100000):
        self.data = data.reset_index(drop=True) 
        self.strategy = strategy
        self.initial_capital = initial_capital
        self.buy_signals = []
        self.sell_signals = []

    def run(self):
        capital = self.initial_capital
        position = 0  # 持有股票数量
        for i in range(len(self.data)):
            price = self.data['price'].iloc[i]
            symbol = self.data['symbol'].iloc[i]
            time = self.data['time'].iloc[i]
            signal = self.strategy.generate_signal(i)

            if signal == 1:  # 买入信号
                # 以资本全部买入股票
                if capital >= price:
                    units = capital // price
                    if units > 0:
                        position += units
                        capital -= units * price
                        self.buy_signals.append((time, price))
                        print(f"买入：时间={time}，价格={price}，持仓={position}股")
            elif signal == -1:  # 卖出信号
                if position > 0:
                    capital += position * price
                    self.sell_signals.append((time, price))
                    print(f"卖出：时间={time}，价格={price}，卖出股数={position}")
                    position = 0  # 清仓
        final_value = capital + position * self.data['price'].iloc[-1]
        print(f"回测结束，资本= {final_value:.2f}")




#test
df1 = load_data('510300', '20250718', '20250719', 'tick')
strategy = Strategy(df1)
backtest = Backtest(df1, strategy)
backtest.run()