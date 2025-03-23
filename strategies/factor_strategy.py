from strategies.base_strategy import BaseStrategy
import pandas as pd
import numpy as np
from typing import Dict, List

class SimpleFactorStrategy(BaseStrategy):
    def __init__(self, data_source, params: dict):
        super().__init__(data_source)
        self.params = params
        # 默认参数
        self.lookback_period = params.get('lookback_period', 20)  # 动量回看期
        self.holding_period = params.get('holding_period', 5)     # 持仓周期
        self.position_size = params.get('position_size', 0.3)     # 仓位大小
        self.symbols = params.get('symbols', [])                  # 股票池
        self.start_date = params.get('start_date')               # 开始日期
        self.end_date = params.get('end_date')                   # 结束日期
        self.cash = params.get('initial_capital', 1000000)       # 初始资金，默认100万
        
    def initialize(self):
        """策略初始化"""
        print(f"\n=== 策略初始化 ===")
        print(f"初始资金: {self.cash:,.2f}")
        
        # 获取历史数据
        self.history_data = {}
        for symbol in self.symbols:
            df = self.data_source.get_daily_data(
                symbol,
                start_date=self.params['start_date'],
                end_date=self.params['end_date']
            )
            if df is not None and not df.empty:
                self.history_data[symbol] = df
                print(f"获取到 {symbol} 的历史数据: {len(df)} 条")
        
        # 初始化因子数据
        self.factor_data = self._prepare_factors()
        
    def _prepare_factors(self) -> Dict[str, pd.DataFrame]:
        """准备动量因子数据"""
        factor_data = {}
        for symbol in self.symbols:
            df = self.history_data.get(symbol)
            if df is not None and not df.empty:
                try:
                    print(f"\n处理 {symbol} 的因子数据...")
                    # 计算收益率
                    df['returns'] = df['close'].pct_change()
                    print(f"收益率范围: {df['returns'].min():.4f} 到 {df['returns'].max():.4f}")
                    
                    # 计算动量因子 (过去N日收益)
                    df['momentum'] = df['returns'].rolling(
                        window=self.lookback_period, 
                        min_periods=1
                    ).sum()
                    
                    print(f"数据点数: {len(df)}")
                    print(f"动量因子范围: {df['momentum'].min():.4f} 到 {df['momentum'].max():.4f}")
                    print(f"非空动量因子数: {df['momentum'].count()}")
                    factor_data[symbol] = df
                except Exception as e:
                    print(f"计算 {symbol} 因子时出错: {str(e)}")
                    import traceback
                    print(traceback.format_exc())
        return factor_data
        
    def _get_current_factors(self, bar: pd.DataFrame) -> Dict[str, float]:
        """获取当前因子值"""
        current_factors = {}
        current_date = bar.index[0]
        print(f"\n当前日期: {current_date}")
        
        for symbol in self.symbols:
            if symbol in self.factor_data:
                df = self.factor_data[symbol]
                if current_date in df.index:
                    momentum = df.loc[current_date, 'momentum']
                    if not pd.isna(momentum):  # 确保不是 NaN
                        current_factors[symbol] = momentum
                        print(f"{symbol} 动量因子值: {momentum:.4f}")
                    else:
                        print(f"警告: {symbol} 在 {current_date} 的动量因子为 NaN")
                else:
                    print(f"警告: {symbol} 没有 {current_date} 的数据")
        return current_factors
        
    def _generate_signals(self, current_factors: Dict[str, float]) -> Dict[str, int]:
        """生成交易信号"""
        signals = {}
        if not current_factors:  # 如果没有有效的因子值，返回空信号
            return signals
        
        # 按动量因子排序
        sorted_symbols = sorted(
            current_factors.items(), 
            key=lambda x: x[1], 
            reverse=True
        )
        
        # 选择动量最强的前20%股票买入
        selection_ratio = 0.2  # 可配置参数
        min_stocks = 1         # 最小持仓数
        n_stocks = max(min_stocks, int(len(sorted_symbols)*selection_ratio))   
        for symbol, momentum in sorted_symbols:
            if len(signals) < n_stocks and momentum > 0:  # 可以添加更严格的动量阈值
                signals[symbol] = 1
                print(f"生成买入信号: {symbol}, 动量值: {momentum:.4f}")
            elif symbol in self.positions:
                if momentum < -0.02:  # 添加止损阈值
                    signals[symbol] = -1
                    print(f"生成卖出信号: {symbol}, 动量值: {momentum:.4f}")
            else:
                signals[symbol] = 0
        
        return signals
        
    def _execute_trades(self, signals: Dict[str, int]):
        """执行交易"""
        print("\n执行交易...")
        for symbol, signal in signals.items():
            if symbol not in self.factor_data:
                print(f"警告: {symbol} 不在因子数据中")
                continue
            
            df = self.factor_data[symbol]
            current_date = self.current_time
            if current_date not in df.index:
                print(f"警告: {symbol} 没有当前日期 {current_date} 的数据")
                continue
            
            current_price = df.loc[current_date, 'close']
            print(f"{symbol} 信号: {signal}, 当前价格: {current_price:.2f}")
            
            if signal == 1 and symbol not in self.positions:
                # 计算购买数量
                position_value = self.cash * self.position_size
                quantity = int(position_value / current_price)
                print(f"尝试买入 {symbol}: 数量={quantity}, 价格={current_price:.2f}")
                if quantity > 0:
                    if self.buy(symbol, quantity, current_price):
                        print(f"买入成功: {symbol} x {quantity}")
                    else:
                        print(f"买入失败: 资金不足")
                    
            elif signal == -1 and symbol in self.positions:
                # 清仓
                quantity = self.positions[symbol]
                print(f"尝试卖出 {symbol}: 数量={quantity}, 价格={current_price:.2f}")
                if quantity > 0:
                    if self.sell(symbol, quantity, current_price):
                        print(f"卖出成功: {symbol} x {quantity}")
                    else:
                        print(f"卖出失败")

    def on_bar(self, bar: pd.DataFrame):
        """K线更新时的回调"""
        # 获取当前的因子值
        current_factors = self._get_current_factors(bar)
        
        # 生成交易信号
        signals = self._generate_signals(current_factors)
        
        # 执行交易
        self._execute_trades(signals) 