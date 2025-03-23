from abc import ABC, abstractmethod
import pandas as pd
from typing import Optional, Dict
from datetime import datetime

class BaseDataSource(ABC):
    """数据源基类"""
    
    @abstractmethod
    def get_daily_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取日线数据"""
        pass
    
    @abstractmethod
    def get_min_data(self, symbol: str, freq: str = '1min') -> pd.DataFrame:
        """获取分钟数据"""
        pass
    
    @abstractmethod
    def get_realtime_data(self, symbol: str) -> pd.DataFrame:
        """获取实时数据"""
        pass
    
    @abstractmethod
    def get_stock_info(self) -> pd.DataFrame:
        """获取股票基本信息"""
        pass
    
    @abstractmethod
    def get_trade_calendar(self, start_date: str, end_date: str) -> pd.DataFrame:
        """获取交易日历"""
        pass
    
    @abstractmethod
    def get_industry_info(self, symbol: Optional[str] = None) -> pd.DataFrame:
        """获取行业分类数据"""
        pass
    
    @abstractmethod
    def get_financial_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取财务数据"""
        pass
    
    @abstractmethod
    def get_tick_data(self, symbol: str, trade_date: str) -> pd.DataFrame:
        """获取逐笔成交数据"""
        pass
    
    @abstractmethod
    def get_level2_quotes(self, symbol: str) -> pd.DataFrame:
        """获取Level2行情"""
        pass

class QlibDataSource(BaseDataSource):
    """Qlib数据源适配器"""
    def __init__(self):
        from qlib.data import D
        self.provider = D
        
    def get_daily_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        instruments = [symbol]
        fields = ['$close', '$volume', '$factor', '$trade_date']
        df = self.provider.features(instruments, fields, start_date, end_date)
        return self._convert_to_standard_format(df)

class RQDataSource(BaseDataSource):
    """RQData数据源适配器"""
    def __init__(self):
        from rqdata import init
        init()
        
    def get_daily_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        # RQData接口调用
        pass 