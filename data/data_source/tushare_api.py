from typing import Dict, List
import tushare as ts
from datetime import datetime
import pandas as pd

class TushareAPI:
    def __init__(self, api_key: str):
        self.api = ts.pro_api(api_key)
        
    def get_daily_data(self, symbol: str, 
                      start_date: str, 
                      end_date: str) -> pd.DataFrame:
        """获取日线数据"""
        try:
            print(f"获取 {symbol} 的数据: {start_date} 到 {end_date}")
            
            # 使用 daily_basic 获取更多的基础数据
            df_daily = self.api.daily(
                ts_code=symbol, 
                start_date=start_date,
                end_date=end_date,
                fields='ts_code,trade_date,open,high,low,close,vol,amount'
            )
            
            if df_daily is None or df_daily.empty:
                print(f"无法获取股票 {symbol} 的数据")
                return pd.DataFrame()
            
            print(f"获取到 {len(df_daily)} 条数据")
            print(f"原始数据前5行:\n{df_daily.head()}")
            
            # 重要：调整数据格式
            df_daily['trade_date'] = pd.to_datetime(df_daily['trade_date'])
            df_daily.set_index('trade_date', inplace=True)
            df_daily.sort_index(ascending=True, inplace=True)  # 按时间正序排列
            
            # 确保数值类型正确
            numeric_columns = ['open', 'high', 'low', 'close', 'vol', 'amount']
            for col in numeric_columns:
                df_daily[col] = pd.to_numeric(df_daily[col], errors='coerce')
            
            # 检查数据的连续性
            date_diff = df_daily.index.to_series().diff().dt.days
            if date_diff.max() > 5:  # 如果有超过5天的数据缺失
                print(f"警告: 数据存在较大间隔，最大间隔: {date_diff.max()}天")
            
            # 打印处理后的数据样本
            print(f"\n处理后的数据前5行:\n{df_daily.head()}")
            print(f"处理后的数据后5行:\n{df_daily.tail()}")
            print(f"数据类型:\n{df_daily.dtypes}")
            print(f"数据统计:\n{df_daily.describe()}")
            
            return df_daily
        
        except Exception as e:
            print(f"获取数据时出错: {str(e)}")
            import traceback
            print(f"错误详情:\n{traceback.format_exc()}")
            return pd.DataFrame()
    
    def get_basic_info(self, symbol: str) -> Dict:
        """获取股票基本信息"""
        return self.api.stock_basic(ts_code=symbol) 
    
    def get_latest_price(self, symbol: str) -> float:
        """获取最新价格"""
        df = self.api.daily(ts_code=symbol, limit=1)
        if df is not None and not df.empty:
            return df['close'].iloc[0]
        return 0.0
    