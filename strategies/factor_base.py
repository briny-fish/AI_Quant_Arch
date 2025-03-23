from abc import ABC, abstractmethod
from typing import Dict, List
import pandas as pd

class BaseFactor(ABC):
    def __init__(self, params: dict):
        self.params = params
        
    @abstractmethod
    def calculate(self, data: pd.DataFrame) -> pd.Series:
        """计算因子值"""
        pass
    
    @abstractmethod
    def generate_signal(self, factor_value: float) -> int:
        """生成交易信号"""
        pass

    # 添加因子评价方法
    def evaluate(self) -> Dict[str, float]:
        pass
    
    # 添加因子预处理
    def preprocess(self, data: pd.DataFrame) -> pd.DataFrame:
        pass

class MomentumFactor(BaseFactor):
    def calculate(self, data: pd.DataFrame) -> pd.Series:
        """计算动量因子"""
        returns = data['close'].pct_change()
        return returns.rolling(
            window=self.params.get('lookback_period', 20),
            min_periods=1
        ).sum() 