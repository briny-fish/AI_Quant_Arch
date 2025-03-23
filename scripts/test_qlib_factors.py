from data.data_source.qlib_source import QlibDataSource
from factors.qlib_factors import QlibMomentumFactor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_qlib_factors():
    try:
        # 初始化数据源
        data_source = QlibDataSource()
        logger.info("数据源初始化成功")
        
        # 测试因子
        params = {'lookback_period': 20}
        factor = QlibMomentumFactor(params)
        
        # 获取测试数据
        symbol = '000001.SZ'
        start_date = '20230101'
        end_date = '20231231'
        
        logger.info(f"获取测试数据: {symbol}, {start_date} - {end_date}")
        df = data_source.get_daily_data(symbol, start_date, end_date)
        
        if df is not None and not df.empty:
            # 计算因子值
            factor_values = factor.calculate(df)
            logger.info("\n因子值示例:")
            logger.info(factor_values.head())
            
            # 获取Alpha158因子集
            alpha158_df = factor.get_alpha158_factors(symbol, start_date, end_date)
            logger.info(f"\nAlpha158因子集包含 {len(alpha158_df.columns)} 个因子")
            logger.info("因子列表:")
            logger.info(alpha158_df.columns.tolist()[:5])  # 显示前5个因子
            
            return True
    except Exception as e:
        logger.error(f"测试失败: {str(e)}")
        return False

if __name__ == "__main__":
    test_qlib_factors() 