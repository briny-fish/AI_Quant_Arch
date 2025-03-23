from typing import Dict, List
import pandas as pd
from qlib.data import D
from strategies.factor_base import BaseFactor

class QlibFactorMixin:
    """Qlib因子混入类"""
    
    def __init__(self):
        self.provider = D
        
    def get_alpha158_factors(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取Alpha158因子集"""
        # Alpha158是Qlib内置的常用因子集
        fields = []
        for group in ['Alpha158']:
            fields.extend(D.list_factors(group=group))
            
        df = D.features(
            instruments=[symbol],
            fields=fields,
            start_time=start_date,
            end_time=end_date,
            freq='day'
        )
        return df

class QlibMomentumFactor(BaseFactor, QlibFactorMixin):
    """使用Qlib实现的动量因子"""
    
    def __init__(self, params: dict):
        BaseFactor.__init__(self, params)
        QlibFactorMixin.__init__(self)
        
    def calculate(self, data: pd.DataFrame) -> pd.Series:
        """计算动量因子"""
        symbol = data.index.get_level_values(1)[0]
        start_date = data.index.get_level_values(0)[0]
        end_date = data.index.get_level_values(0)[-1]
        
        # 使用Qlib的动量因子
        fields = ['momentum_20']
        df = self.provider.features(
            instruments=[symbol],
            fields=fields,
            start_time=start_date,
            end_time=end_date,
            freq='day'
        )
        return df['momentum_20'] 