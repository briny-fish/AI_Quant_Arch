import logging
import schedule
import time
from typing import List
from data.data_source.tushare_source import TushareDataSource
from data.storage.market_data import MarketDataStorage
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MarketDataManager:
    def __init__(self, token: str, symbols: List[str]):
        self.data_source = TushareDataSource(token)
        self.storage = MarketDataStorage()
        self.symbols = symbols
        self.is_trading_time = False
        
    def _is_trading_time(self) -> bool:
        """判断是否为交易时段"""
        now = datetime.now()
        
        # 如果是周末，不是交易时段
        if now.weekday() >= 5:
            return False
            
        # 判断是否在交易时间段内
        morning_start = now.replace(hour=9, minute=30, second=0)
        morning_end = now.replace(hour=11, minute=30, second=0)
        afternoon_start = now.replace(hour=13, minute=0, second=0)
        afternoon_end = now.replace(hour=15, minute=0, second=0)
        
        if (morning_start <= now <= morning_end) or \
           (afternoon_start <= now <= afternoon_end):
            return True
            
        return False
        
    def update_daily_data(self):
        """更新日线数据"""
        logger.info("开始更新日线数据...")
        try:
            self.storage.update_daily_data(self.data_source)
            logger.info("日线数据更新完成")
        except Exception as e:
            logger.error(f"日线数据更新失败: {str(e)}")
    
    def update_minute_data(self):
        """更新分钟数据"""
        if not self._is_trading_time():
            logger.info("非交易时段，跳过更新")
            return
            
        logger.info("开始更新分钟数据...")
        try:
            self.storage.update_minute_data(self.data_source, self.symbols)
            logger.info("分钟数据更新完成")
        except Exception as e:
            logger.error(f"分钟数据更新失败: {str(e)}")
    
    def cleanup_data(self):
        """清理过期数据"""
        logger.info("开始清理过期数据...")
        try:
            self.storage.cleanup_old_data()
            logger.info("数据清理完成")
        except Exception as e:
            logger.error(f"数据清理失败: {str(e)}")
    
    def backup_data(self):
        """备份数据"""
        try:
            self.storage.backup_database()
        except Exception as e:
            logger.error(f"数据备份失败: {str(e)}")
            
    def schedule_tasks(self):
        """调度任务"""
        # 每天更新日线数据
        schedule.every().day.at("17:00").do(self.update_daily_data)
        
        # 交易时段更新分钟数据
        schedule.every(1).minutes.do(self.update_minute_data)
        
        # 每天清理过期数据
        schedule.every().day.at("00:00").do(self.cleanup_data)
        
        # 每天备份数据
        schedule.every().day.at("16:30").do(self.backup_data)
        
        while True:
            try:
                schedule.run_pending()
                time.sleep(1)
            except Exception as e:
                logger.error(f"任务执行失败: {str(e)}")
                time.sleep(60)  # 发生错误时等待较长时间

if __name__ == "__main__":
    # 配置
    TUSHARE_TOKEN = "your_token_here"
    SYMBOLS = ['000001.SZ', '600000.SH']  # 示例股票
    
    manager = MarketDataManager(TUSHARE_TOKEN, SYMBOLS)
    manager.schedule_tasks() 