import tushare as ts
import pandas as pd
from datetime import datetime
from data.data_source.base import BaseDataSource
from utils.retry import retry_on_error
import time
from config.base_config import current_config

class TushareDataSource(BaseDataSource):
    """Tushare数据源适配器"""
    
    def __init__(self, token: str):
        self.pro = ts.pro_api(token)
        self.ts = ts
        self.last_call_time = 0  # 新增调用间隔控制
        self.call_count = 0
        
    @retry_on_error(max_retries=3, delay=2.0)
    def get_daily_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取日线数据"""
        if self.call_count >= current_config.MAX_API_CALLS:
            raise Exception("达到每日API调用上限")
        self.call_count += 1
        
        # 控制调用频率
        time_since_last = time.time() - self.last_call_time
        if time_since_last < 1:  # 至少1秒间隔
            time.sleep(1 - time_since_last)
        self.last_call_time = time.time()
        
        df = self.pro.daily(
            ts_code=symbol,
            start_date=start_date,
            end_date=end_date,
            fields='ts_code,trade_date,open,high,low,close,vol,amount'
        )
        return self._convert_to_standard_format(df)
    
    @retry_on_error(max_retries=3, delay=1.0)
    def get_realtime_data(self, symbol: str) -> pd.DataFrame:
        """获取实时数据"""
        df = self.pro.quotes(ts_code=symbol)
        return self._convert_realtime_to_standard(df)
        
    def get_min_data(self, symbol: str, freq: str = '1min') -> pd.DataFrame:
        """获取分钟数据
        freq: 1min, 5min, 15min, 30min, 60min
        """
        df = self.pro.stk_mins(
            ts_code=symbol,
            freq=freq,
            fields='ts_code,trade_time,open,high,low,close,vol,amount'
        )
        return self._convert_to_standard_format(df)
    
    @retry_on_error(max_retries=3, delay=2.0)
    def get_stock_info(self) -> pd.DataFrame:
        """获取股票基本信息"""
        df = self.pro.stock_basic(
            exchange='',
            list_status='L',
            fields='ts_code,symbol,name,area,industry,list_date,market,is_hs'
        )
        
        # 获取ST股票信息
        st_df = self.pro.stock_basic(
            exchange='',
            list_status='L',
            fields='ts_code,name'
        )
        st_stocks = st_df[st_df['name'].str.contains('ST')]['ts_code'].tolist()
        
        df['is_st'] = df['ts_code'].isin(st_stocks)
        df['is_active'] = True  # 默认上市状态为活跃
        
        return df
        
    @retry_on_error(max_retries=3, delay=1.0)
    def get_tick_data(self, symbol: str, trade_date: str) -> pd.DataFrame:
        """获取逐笔成交数据"""
        df = self.pro.stk_tick(
            ts_code=symbol,
            trade_date=trade_date,
            fields='ts_code,trade_time,price,vol,amount,trade_type'
        )
        return self._convert_tick_to_standard(df)
        
    @retry_on_error(max_retries=3, delay=1.0)
    def get_level2_quotes(self, symbol: str) -> pd.DataFrame:
        """获取Level2行情"""
        df = self.pro.level2_quotes(
            ts_code=symbol,
            fields='ts_code,trade_time,bid1,bid2,bid3,bid4,bid5,ask1,ask2,ask3,ask4,ask5,' +
                  'bid1_vol,bid2_vol,bid3_vol,bid4_vol,bid5_vol,' +
                  'ask1_vol,ask2_vol,ask3_vol,ask4_vol,ask5_vol'
        )
        return self._convert_level2_to_standard(df)
    
    @retry_on_error(max_retries=3, delay=2.0)
    def get_trade_calendar(self, start_date: str, end_date: str) -> pd.DataFrame:
        """获取交易日历"""
        df = self.pro.trade_cal(
            exchange='SSE',
            start_date=start_date,
            end_date=end_date,
            fields='cal_date,is_open,pretrade_date'
        )
        return self._convert_calendar_to_standard(df)
    
    @retry_on_error(max_retries=3, delay=2.0)
    def get_industry_info(self, symbol: str = None) -> pd.DataFrame:
        """获取行业分类数据"""
        if symbol:
            df = self.pro.stock_basic(
                ts_code=symbol,
                fields='ts_code,industry,market'
            )
        else:
            df = self.pro.stock_basic(
                fields='ts_code,industry,market'
            )
        return self._convert_industry_to_standard(df)
    
    @retry_on_error(max_retries=3, delay=2.0)
    def get_financial_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取财务数据"""
        try:
            # 获取资产负债表数据
            df_balance = self.pro.balancesheet(
                ts_code=symbol,
                start_date=start_date,
                end_date=end_date,
                fields='ts_code,end_date,total_assets,total_liab,current_ratio'
            )
            
            # 获取利润表数据
            df_income = self.pro.income(
                ts_code=symbol,
                start_date=start_date,
                end_date=end_date,
                fields='ts_code,end_date,ann_date,total_revenue,net_profit,roe,asset_turn'
            )
            
            # 合并两个数据源
            merged_df = pd.merge(df_income, df_balance, on=['ts_code', 'end_date'], how='outer')
            return self._convert_financial_format(merged_df)
        except Exception as e:
            print(f"获取财务数据失败: {str(e)}")
            return pd.DataFrame()
    
    def _convert_to_standard_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """转换为标准格式"""
        df = df.copy()
        
        # 统一字段映射关系
        rename_dict = {
            'ts_code': 'symbol',
            'vol': 'volume',  # 确保成交量字段转换
            'amount': 'amount',
            'trade_date': 'trade_date',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close'
        }
        
        # 只重命名存在的列
        existing_columns = [col for col in rename_dict.keys() if col in df.columns]
        df.rename(columns={k: v for k, v in rename_dict.items() if k in existing_columns}, inplace=True)
        
        # 确保包含所有必要字段
        required_columns = ['symbol', 'trade_date', 'open', 'high', 'low', 'close', 'volume']
        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"转换后数据缺少必要字段: {col}")
        
        return df
    
    def _convert_realtime_to_standard(self, df: pd.DataFrame) -> pd.DataFrame:
        """转换实时数据为标准格式"""
        df = df.copy()
        rename_dict = {
            'vol': 'volume',
            'trade_time': 'time',
            'current': 'close',
            'bid1': 'bid_price1',
            'ask1': 'ask_price1',
            'bid1_vol': 'bid_volume1',
            'ask1_vol': 'ask_volume1'
        }
        df.rename(columns=rename_dict, inplace=True)
        return df
    
    def _convert_tick_to_standard(self, df: pd.DataFrame) -> pd.DataFrame:
        """转换逐笔数据为标准格式"""
        df = df.copy()
        rename_dict = {
            'vol': 'volume',
            'trade_time': 'time'
        }
        df.rename(columns=rename_dict, inplace=True)
        
        # 转换时间格式
        df['time'] = pd.to_datetime(df['time'])
        return df
    
    def _convert_level2_to_standard(self, df: pd.DataFrame) -> pd.DataFrame:
        """转换Level2数据为标准格式"""
        df = df.copy()
        
        # 重命名列
        level2_rename = {
            'trade_time': 'time',
            'bid1': 'bid_price1',
            'bid2': 'bid_price2',
            'bid3': 'bid_price3',
            'bid4': 'bid_price4',
            'bid5': 'bid_price5',
            'ask1': 'ask_price1',
            'ask2': 'ask_price2',
            'ask3': 'ask_price3',
            'ask4': 'ask_price4',
            'ask5': 'ask_price5',
            'bid1_vol': 'bid_volume1',
            'bid2_vol': 'bid_volume2',
            'bid3_vol': 'bid_volume3',
            'bid4_vol': 'bid_volume4',
            'bid5_vol': 'bid_volume5',
            'ask1_vol': 'ask_volume1',
            'ask2_vol': 'ask_volume2',
            'ask3_vol': 'ask_volume3',
            'ask4_vol': 'ask_volume4',
            'ask5_vol': 'ask_volume5'
        }
        df.rename(columns=level2_rename, inplace=True)
        
        # 转换时间格式
        df['time'] = pd.to_datetime(df['time'])
        return df
    
    def _convert_calendar_to_standard(self, df: pd.DataFrame) -> pd.DataFrame:
        """转换交易日历为标准格式"""
        df = df.copy()
        df['date'] = pd.to_datetime(df['cal_date'])
        df['prev_trade_date'] = pd.to_datetime(df['pretrade_date'])
        df.set_index('date', inplace=True)
        return df
    
    def _convert_industry_to_standard(self, df: pd.DataFrame) -> pd.DataFrame:
        """转换行业数据为标准格式"""
        df = df.copy()
        # 修正必要字段检查
        required_columns = ['ts_code', 'industry', 'market']  # 使用实际存在的字段
        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"原始数据缺少必要字段: {col}")
        
        rename_dict = {
            'ts_code': 'symbol',
            'industry': 'industry_name',
            'market': 'industry_type'  # 使用market字段替代
        }
        df.rename(columns=rename_dict, inplace=True)
        return df
    
    def _convert_financial_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """转换财务数据格式"""
        if df.empty:
            return df
        
        rename_dict = {
            'ts_code': 'symbol',
            'end_date': 'report_date',
            'ann_date': 'announce_date',
            'n_income': 'net_income',
            'assets_turn': 'asset_turnover',
            'total_assets': 'total_assets',
            'total_liab': 'total_liab'
        }
        
        # 处理可能存在的空值
        required_fields = ['ts_code', 'net_profit', 'total_assets', 'total_liab']
        df = df.dropna(subset=required_fields)
        
        # 验证必要字段
        for field in required_fields:
            if field not in df.columns:
                raise ValueError(f"财务数据缺少必要字段: {field}")
        
        df.rename(columns=rename_dict, inplace=True)
        return df