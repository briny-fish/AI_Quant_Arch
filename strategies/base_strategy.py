from abc import ABC, abstractmethod
from typing import Dict, List, Optional
import pandas as pd
from datetime import datetime

class BaseStrategy(ABC):
    def __init__(self, data_source):
        self.data_source = data_source
        self.positions: Dict[str, int] = {}        # 当前持仓 {symbol: quantity}
        self.positions_value: Dict[str, float] = {} # 持仓市值 {symbol: value}
        self.cash: float = 0.0                     # 可用资金
        self.trades: List[Dict] = []               # 交易记录
        self.current_time: Optional[datetime] = None # 当前回测时间点
        
    @abstractmethod
    def initialize(self):
        """策略初始化"""
        pass
        
    @abstractmethod
    def on_bar(self, bar: pd.DataFrame):
        """K线更新时的回调"""
        pass
    
    def buy(self, symbol: str, quantity: int, price: float):
        """买入接口"""
        cost = quantity * price
        if cost > self.cash:
            return False
            
        # 更新现金和持仓
        self.cash -= cost
        self.positions[symbol] = self.positions.get(symbol, 0) + quantity
        
        # 记录交易
        trade = {
            'time': self.current_time,
            'symbol': symbol,
            'direction': 'buy',
            'quantity': quantity,
            'price': price,
            'cost': cost
        }
        self.trades.append(trade)
        return True
        
    def sell(self, symbol: str, quantity: int, price: float):
        """卖出接口"""
        if symbol not in self.positions or self.positions[symbol] < quantity:
            return False
            
        # 更新现金和持仓
        revenue = quantity * price
        self.cash += revenue
        self.positions[symbol] -= quantity
        
        # 如果持仓为0，删除该股票的持仓记录
        if self.positions[symbol] == 0:
            del self.positions[symbol]
            
        # 记录交易
        trade = {
            'time': self.current_time,
            'symbol': symbol,
            'direction': 'sell',
            'quantity': quantity,
            'price': price,
            'revenue': revenue
        }
        self.trades.append(trade)
        return True
        
    def get_position(self, symbol: str) -> int:
        """获取持仓数量"""
        return self.positions.get(symbol, 0)
        
    def get_positions_value(self) -> float:
        """获取当前持仓市值"""
        total_value = 0.0
        for symbol, quantity in self.positions.items():
            current_price = self.data_source.get_latest_price(symbol)
            total_value += quantity * current_price
        return total_value
        
    def get_total_value(self) -> float:
        """获取总资产"""
        return self.cash + self.get_positions_value() 