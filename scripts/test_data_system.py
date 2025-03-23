import logging
import os
import time
from datetime import datetime, timedelta
from data.data_source.tushare_source import TushareDataSource
from data.storage.market_data import MarketDataStorage
from data.storage.market_stats import MarketStats
import pandas as pd
from config.base_config import current_config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataSystemTester:
    def __init__(self, token: str, test_db_path: str = 'data/test_market.db'):
        self.data_source = TushareDataSource(token)
        self.storage = MarketDataStorage(db_path=test_db_path)
        self.stats = MarketStats(self.storage)
        
    def test_basic_data(self):
        """测试基础数据获取和存储"""
        logger.info("=== 测试基础数据 ===")
        
        # 测试股票基本信息
        logger.info("1. 测试股票基本信息更新...")
        self.storage.update_stock_info(self.data_source)
        
        # 测试交易日历
        logger.info("2. 测试交易日历更新...")
        start_date = '20230101'
        end_date = '20231231'
        self.storage.update_trade_calendar(
            self.data_source, 
            start_date, 
            end_date
        )
        
        # 测试行业数据
        logger.info("3. 测试行业数据更新...")
        self.storage.update_industry_info(self.data_source)
        
    def test_market_data(self):
        """测试行情数据"""
        logger.info("\n=== 测试行情数据 ===")
        
        # 获取测试股票
        test_symbols = ['000001.SZ', '600000.SH']
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
        
        # 测试日线数据
        logger.info("1. 测试日线数据更新...")
        for symbol in test_symbols:
            df = self.data_source.get_daily_data(symbol, start_date, end_date)
            if df is not None and not df.empty:
                self.storage._save_daily_data(symbol, df)
                logger.info(f"{symbol}: 获取到 {len(df)} 条日线数据")
        
        # 注释掉分钟数据测试（避免触发API限制）
        # logger.info("\n2. 测试分钟数据更新...")
        # for symbol in test_symbols:
        #     df = self.data_source.get_min_data(symbol, freq='1min')
        #     if df is not None and not df.empty:
        #         self.storage._save_minute_data(symbol, df, '1min')
        #         logger.info(f"{symbol}: 获取到 {len(df)} 条分钟数据")
        
        logger.info("\n2. 分钟数据测试已跳过（避免API限制）")
        
    def test_financial_data(self):
        """测试财务数据"""
        logger.info("\n=== 测试财务数据 ===")
        
        test_symbol = '000001.SZ'
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
        
        logger.info(f"更新财务数据: {test_symbol}")
        self.storage.update_financial_data(
            self.data_source,
            test_symbol,
            start_date,
            end_date
        )
        
        # 获取并显示财务指标
        indicators = self.storage.get_financial_indicators(test_symbol)
        logger.info(f"财务指标示例:\n{indicators.head()}")
        
    def test_data_quality(self):
        """测试数据质量"""
        logger.info("\n=== 测试数据质量 ===")
        
        # 检查数据一致性
        consistency = self.storage.check_data_consistency()
        
        logger.info("1. 交易日历统计:")
        logger.info(consistency['calendar'])
        
        logger.info("\n2. 日线数据统计:")
        for record in consistency['daily_data'][:5]:  # 显示前5条
            logger.info(record)
            
        logger.info("\n3. 财务数据统计:")
        for record in consistency['financial_data'][:5]:  # 显示前5条
            logger.info(record)
            
    def test_data_query(self):
        """测试数据查询"""
        logger.info("\n=== 测试数据查询 ===")
        
        # 测试行业成分股查询
        industry = '银行'  # 示例行业
        stocks = self.storage.get_industry_stocks(industry)
        logger.info(f"\n1. {industry}行业成分股: {stocks[:5]}")  # 显示前5个
        
        # 测试交易日期查询
        start_date = '20230101'
        end_date = '20230131'
        trading_dates = self.storage.get_trading_dates(start_date, end_date)
        logger.info(f"\n2. 交易日期示例: {trading_dates[:5]}")
        
        # 测试K线数据查询
        symbol = '000001.SZ'
        kline = self.storage.get_kline_data(symbol, freq='1d', limit=5)
        logger.info(f"\n3. K线数据示例:\n{kline}")
        
    def test_incremental_update(self):
        """测试增量更新功能"""
        logger.info("\n=== 测试增量更新 ===")
        
        # 第一次全量更新
        test_symbol = '600036.SH'
        self.storage.update_daily_data(self.data_source)
        initial_count = self._get_symbol_data_count(test_symbol)
        
        # 模拟新数据生成
        new_data = self.data_source.get_daily_data(
            test_symbol, 
            start_date=(datetime.now() - timedelta(days=5)).strftime('%Y%m%d'),
            end_date=datetime.now().strftime('%Y%m%d')
        )
        
        # 第二次增量更新
        self.storage.update_daily_data(self.data_source)
        updated_count = self._get_symbol_data_count(test_symbol)
        
        # 验证增量结果
        assert updated_count == initial_count + len(new_data), "增量更新数据量异常"
        logger.info("增量更新测试通过")

    def _get_symbol_data_count(self, symbol: str) -> int:
        """获取指定股票的数据量"""
        with self.storage._get_connection() as conn:
            return pd.read_sql(
                f"SELECT COUNT(*) FROM daily_price WHERE symbol = '{symbol}'",
                conn
            ).iloc[0,0]

    def test_data_continuity(self):
        """测试数据连续性"""
        logger.info("\n=== 测试数据连续性 ===")
        
        test_symbol = '000001.SZ'
        with self.storage._get_connection() as conn:
            df = pd.read_sql(
                f"""SELECT trade_date FROM daily_price 
                   WHERE symbol = '{test_symbol}'
                   ORDER BY trade_date""",
                conn
            )
        
        # 转换日期格式
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        
        # 检查日期连续性
        date_diff = df['trade_date'].diff().dt.days.dropna()
        discontinuity = date_diff[date_diff > 1]  # 允许周末休市
        
        if not discontinuity.empty:
            logger.error(f"发现数据不连续间隔: {discontinuity.tolist()}")
            assert False, "存在数据不连续问题"
        
        logger.info("数据连续性检查通过")

    def test_invalid_data_handling(self):
        """测试异常数据处理"""
        logger.info("\n=== 测试异常数据拦截 ===")
        
        # 构造异常数据
        invalid_data = pd.DataFrame({
            'trade_date': ['20231001', '20231002'],
            'open': [15.6, 16.2],
            'high': [15.5, 16.1],  # 最高价低于开盘价
            'low': [15.4, 16.0],
            'close': [15.5, 16.1],
            'volume': [100000, -200000]  # 负成交量
        })
        
        # 尝试保存异常数据
        try:
            self.storage._save_daily_data('TEST', invalid_data)
            assert False, "异常数据未被正确拦截"
        except Exception as e:
            logger.info(f"成功拦截异常数据: {str(e)}")
        
        # 验证数据库无测试数据
        count = self._get_symbol_data_count('TEST')
        assert count == 0, "异常数据被错误保存"

    def test_api_limit(self):
        """测试API调用限制"""
        logger.info("\n=== 测试API限制 ===")
        
        # 模拟超过调用限制
        self.data_source.call_count = current_config.MAX_API_CALLS
        
        try:
            self.data_source.get_daily_data('000001.SZ', '20230101', '20230110')
            assert False, "未正确触发API限制"
        except Exception as e:
            logger.info(f"成功触发API限制: {str(e)}")

    def run_all_tests(self):
        """运行所有测试"""
        try:
            # 验证配置加载
            logger.info(f"当前配置: MAX_API_CALLS={current_config.MAX_API_CALLS}")
            logger.info(f"测试模式: ENABLE_MINUTE_TEST={current_config.ENABLE_MINUTE_TEST}")
            
            self.test_basic_data()
            self.test_market_data()
            self.test_financial_data()
            self.test_data_quality()
            self.test_data_query()
            self.test_incremental_update()
            self.test_data_continuity()
            self.test_invalid_data_handling()
            self.test_api_limit()
            logger.info("\n所有测试完成!")
            return True
        except Exception as e:
            logger.error(f"测试过程中发生错误: {str(e)}")
            return False

def cleanup_db(db_path: str, max_retries: int = 5):
    """安全清理数据库文件"""
    for i in range(max_retries):
        try:
            if os.path.exists(db_path):
                if os.name == 'nt':  # Windows系统
                    os.system(f"del /F /Q {db_path}")
                else:
                    os.remove(db_path)
                logger.info(f"成功删除数据库文件: {db_path}")
                return True
        except Exception as e:  # 捕获更广泛的异常
            logger.warning(f"尝试删除数据库文件失败 ({i+1}/{max_retries}): {str(e)}")
            time.sleep(2)
    logger.error(f"无法删除数据库文件: {db_path}")
    return False

if __name__ == "__main__":
    TEST_DB_PATH = 'data/test_market.db'
    TUSHARE_TOKEN = "4da2fbec9c2cee373d3aace9f9e200a315a2812dc11267c425010cec"
    
    # 确保测试目录存在
    os.makedirs(os.path.dirname(TEST_DB_PATH), exist_ok=True)
    
    # 清理旧的测试数据库
    cleanup_db(TEST_DB_PATH)
    
    tester = None
    try:
        tester = DataSystemTester(TUSHARE_TOKEN, TEST_DB_PATH)
        success = tester.run_all_tests()
    except Exception as e:
        logger.error(f"测试过程中发生错误: {str(e)}")
        success = False
    finally:
        if tester:
            tester.storage.close_all_connections()
            time.sleep(1)
            cleanup_db(TEST_DB_PATH)
    
    if success:
        logger.info("\n✓ 所有测试通过")
    else:
        logger.error("\n✗ 测试失败")