import logging
from datetime import datetime, timedelta
from data.data_source.tushare_source import TushareDataSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_tushare_data(token: str):
    """测试Tushare数据获取"""
    data_source = TushareDataSource(token)
    
    # 测试股票基本信息
    logger.info("测试获取股票基本信息...")
    stock_info = data_source.get_stock_info()
    logger.info(f"获取到 {len(stock_info)} 只股票的基本信息")
    logger.info(f"数据示例:\n{stock_info.head()}")
    
    # 测试日线数据
    symbol = '000001.SZ'
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
    
    logger.info(f"\n测试获取日线数据: {symbol}")
    daily_data = data_source.get_daily_data(symbol, start_date, end_date)
    logger.info(f"获取到 {len(daily_data)} 条日线数据")
    logger.info(f"数据示例:\n{daily_data.head()}")
    
    # 测试分钟数据
    logger.info(f"\n测试获取分钟数据: {symbol}")
    min_data = data_source.get_min_data(symbol, freq='1min')
    logger.info(f"获取到 {len(min_data)} 条分钟数据")
    logger.info(f"数据示例:\n{min_data.head()}")
    
    # 测试逐笔数据
    logger.info(f"\n测试获取逐笔数据: {symbol}")
    tick_data = data_source.get_tick_data(symbol, end_date)
    logger.info(f"获取到 {len(tick_data)} 条逐笔数据")
    logger.info(f"数据示例:\n{tick_data.head()}")
    
    # 测试Level2行情
    logger.info(f"\n测试获取Level2行情: {symbol}")
    level2_data = data_source.get_level2_quotes(symbol)
    logger.info(f"获取到 {len(level2_data)} 条Level2数据")
    logger.info(f"数据示例:\n{level2_data.head()}")

if __name__ == "__main__":
    TUSHARE_TOKEN = "your_token_here"
    test_tushare_data(TUSHARE_TOKEN) 