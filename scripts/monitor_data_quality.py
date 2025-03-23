import logging
import pandas as pd
from typing import Dict, List
from data.storage.market_data import MarketDataStorage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataQualityMonitor:
    def __init__(self, storage: MarketDataStorage):
        self.storage = storage
        
    def check_data_continuity(self, symbol: str, freq: str = '1d') -> Dict:
        """检查数据连续性"""
        df = self.storage.get_kline_data(symbol, freq=freq, limit=1000)
        
        if df.empty:
            return {'status': 'error', 'message': '无数据'}
            
        # 检查时间间隔
        df['time_diff'] = df.index.to_series().diff()
        max_gap = df['time_diff'].max()
        
        expected_gap = pd.Timedelta('1 day') if freq == '1d' else pd.Timedelta(freq)
        if max_gap > expected_gap * 3:  # 允许最多间隔3个周期
            return {
                'status': 'warning',
                'message': f'数据存在较大间隔: {max_gap}'
            }
            
        return {'status': 'ok', 'message': '数据连续性正常'}
        
    def check_price_validity(self, symbol: str) -> Dict:
        """检查价格有效性"""
        latest = self.storage.get_latest_price(symbol)
        
        if not latest:
            return {'status': 'error', 'message': '无法获取最新价格'}
            
        # 检查价格是否为0或负数
        if latest['close'] <= 0:
            return {
                'status': 'error',
                'message': f'价格异常: {latest["close"]}'
            }
            
        return {'status': 'ok', 'message': '价格数据正常'}
        
    def run_all_checks(self, symbols: List[str]) -> Dict:
        """运行所有检查"""
        results = {}
        for symbol in symbols:
            symbol_result = {
                'continuity': self.check_data_continuity(symbol),
                'price': self.check_price_validity(symbol)
            }
            results[symbol] = symbol_result
        return results

if __name__ == "__main__":
    storage = MarketDataStorage()
    monitor = DataQualityMonitor(storage)
    
    # 测试股票
    symbols = ['000001.SZ', '600000.SH']
    
    # 运行检查
    results = monitor.run_all_checks(symbols)
    
    # 输出结果
    for symbol, checks in results.items():
        print(f"\n检查结果 - {symbol}:")
        for check_name, result in checks.items():
            print(f"{check_name}: {result['status']} - {result['message']}") 