import pandas as pd
from typing import Dict, List
from datetime import datetime, timedelta

class MarketStats:
    def __init__(self, storage):
        self.storage = storage
        
    def get_data_coverage(self, start_date: str, end_date: str) -> Dict:
        """获取数据覆盖率统计"""
        with self.storage._get_connection() as conn:
            # 获取应该有数据的交易日数量
            trading_days = pd.read_sql(
                """
                SELECT COUNT(DISTINCT trade_date) as days
                FROM daily_price
                WHERE trade_date BETWEEN ? AND ?
                """,
                conn,
                params=(start_date, end_date)
            ).iloc[0]['days']
            
            # 获取每只股票的实际数据天数
            stock_coverage = pd.read_sql(
                """
                SELECT symbol, COUNT(*) as data_days
                FROM daily_price
                WHERE trade_date BETWEEN ? AND ?
                GROUP BY symbol
                """,
                conn,
                params=(start_date, end_date)
            )
            
            # 计算覆盖率
            stock_coverage['coverage'] = stock_coverage['data_days'] / trading_days
            
            return {
                'trading_days': trading_days,
                'stock_coverage': stock_coverage.to_dict('records')
            }
            
    def get_data_quality_stats(self) -> Dict:
        """获取数据质量统计"""
        with self.storage._get_connection() as conn:
            # 检查空值
            null_stats = pd.read_sql(
                """
                SELECT 
                    symbol,
                    SUM(CASE WHEN open IS NULL THEN 1 ELSE 0 END) as null_open,
                    SUM(CASE WHEN close IS NULL THEN 1 ELSE 0 END) as null_close,
                    SUM(CASE WHEN volume IS NULL THEN 1 ELSE 0 END) as null_volume
                FROM daily_price
                GROUP BY symbol
                HAVING null_open > 0 OR null_close > 0 OR null_volume > 0
                """,
                conn
            )
            
            # 检查异常值
            abnormal_stats = pd.read_sql(
                """
                SELECT 
                    symbol,
                    SUM(CASE WHEN open <= 0 THEN 1 ELSE 0 END) as invalid_open,
                    SUM(CASE WHEN close <= 0 THEN 1 ELSE 0 END) as invalid_close,
                    SUM(CASE WHEN volume <= 0 THEN 1 ELSE 0 END) as invalid_volume
                FROM daily_price
                GROUP BY symbol
                HAVING invalid_open > 0 OR invalid_close > 0 OR invalid_volume > 0
                """,
                conn
            )
            
            return {
                'null_stats': null_stats.to_dict('records'),
                'abnormal_stats': abnormal_stats.to_dict('records')
            } 