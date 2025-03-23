import time
import logging
from data.data_source.tushare_source import TushareDataSource
from data.storage.market_data import MarketDataStorage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def update_realtime_loop(token: str, symbols: List[str], interval: int = 3):
    """实时数据更新循环"""
    data_source = TushareDataSource(token)
    storage = MarketDataStorage()
    
    while True:
        try:
            logger.info("更新实时数据...")
            storage.update_realtime_data(data_source, symbols)
            time.sleep(interval)  # 休眠3秒
            
        except Exception as e:
            logger.error(f"更新失败: {str(e)}")
            time.sleep(interval)

if __name__ == "__main__":
    # 配置
    TUSHARE_TOKEN = "your_token_here"
    SYMBOLS = ['000001.SZ', '600000.SH']  # 示例股票
    
    update_realtime_loop(TUSHARE_TOKEN, SYMBOLS) 