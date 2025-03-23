from data.data_source.base import BaseDataSource
from qlib.data import D
from qlib.config import REG_CN as REGION_CN
import qlib
import pandas as pd

class QlibDataSource(BaseDataSource):
    """Qlib数据源适配器"""
    
    def __init__(self, provider_name: str = 'cn_data'):
        # 初始化Qlib
        qlib.init(
            provider_name=provider_name,
            region=REGION_CN,
            expression_cache=None,
            calendar_cache=None
        )
        self.provider = D
        
    def get_daily_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取日线数据"""
        fields = [
            '$open', '$high', '$low', '$close', 
            '$volume', '$factor', 
            '$vwap', '$turnover'
        ]
        
        df = self.provider.features(
            instruments=[symbol],
            fields=fields,
            start_time=start_date,
            end_time=end_date,
            freq='day'
        )
        
        return self._convert_to_standard_format(df)
        
    def _convert_to_standard_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """转换为标准格式"""
        df = df.copy()
        # 重命名列
        rename_dict = {
            '$open': 'open',
            '$high': 'high',
            '$low': 'low',
            '$close': 'close',
            '$volume': 'volume',
            '$factor': 'adj_factor',
            '$vwap': 'vwap',
            '$turnover': 'turnover'
        }
        df.rename(columns=rename_dict, inplace=True)
        return df 