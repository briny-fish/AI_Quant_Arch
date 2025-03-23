import logging
from datetime import datetime
from data.data_source.qlib_source import QlibDataSource
from data.storage.market_data import MarketDataStorage
from factors.qlib_factors import QlibMomentumFactor, QlibFactorMixin

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_data_source():
    """测试数据源功能"""
    try:
        data_source = QlibDataSource()
        symbol = '000001.SZ'
        df = data_source.get_daily_data(
            symbol=symbol,
            start_date='20230101',
            end_date='20230131'
        )
        
        logger.info(f"获取到数据shape: {df.shape}")
        logger.info(f"数据列: {df.columns.tolist()}")
        logger.info(f"数据示例:\n{df.head()}")
        return True
    except Exception as e:
        logger.error(f"数据源测试失败: {str(e)}")
        return False

def test_data_storage():
    """测试数据存储功能"""
    try:
        storage = MarketDataStorage()
        data_source = QlibDataSource()
        
        # 测试数据更新
        storage.update_daily_data(data_source)
        
        # 验证存储的数据
        with storage._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM daily_price")
            count = cursor.fetchone()[0]
            logger.info(f"数据库中共有 {count} 条记录")
            
            cursor.execute("""
                SELECT symbol, COUNT(*) as count 
                FROM daily_price 
                GROUP BY symbol 
                LIMIT 5
            """)
            symbol_counts = cursor.fetchall()
            logger.info("每个股票的数据量示例:")
            for symbol, count in symbol_counts:
                logger.info(f"{symbol}: {count}条记录")
        return True
    except Exception as e:
        logger.error(f"数据存储测试失败: {str(e)}")
        return False

def test_factor_calculation():
    """测试因子计算功能"""
    try:
        data_source = QlibDataSource()
        params = {'lookback_period': 20}
        factor = QlibMomentumFactor(params)
        
        # 测试单个股票的因子计算
        symbol = '000001.SZ'
        df = data_source.get_daily_data(
            symbol=symbol,
            start_date='20230101',
            end_date='20230331'
        )
        
        # 计算动量因子
        factor_values = factor.calculate(df)
        logger.info(f"因子值统计:\n{factor_values.describe()}")
        
        # 测试Alpha158因子集
        alpha158_df = factor.get_alpha158_factors(
            symbol=symbol,
            start_date='20230101',
            end_date='20230131'
        )
        logger.info(f"Alpha158因子集大小: {alpha158_df.shape}")
        return True
    except Exception as e:
        logger.error(f"因子计算测试失败: {str(e)}")
        return False

def run_all_tests():
    """运行所有测试"""
    tests = [
        ("数据源测试", test_data_source),
        ("数据存储测试", test_data_storage),
        ("因子计算测试", test_factor_calculation)
    ]
    
    results = []
    for test_name, test_func in tests:
        logger.info(f"\n开始{test_name}...")
        success = test_func()
        results.append((test_name, success))
        logger.info(f"{test_name}完成: {'成功' if success else '失败'}\n")
    
    # 输出总结
    logger.info("\n=== 测试结果总结 ===")
    all_success = True
    for test_name, success in results:
        logger.info(f"{test_name}: {'✓' if success else '✗'}")
        all_success = all_success and success
    
    return all_success

if __name__ == "__main__":
    success = run_all_tests()
    if success:
        logger.info("\n所有测试通过！")
    else:
        logger.error("\n存在失败的测试！") 