import qlib
from qlib.data import D
from qlib.config import REG_CN
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_qlib_data():
    """初始化Qlib数据"""
    logger.info("开始初始化Qlib数据...")
    
    try:
        qlib.init(
            provider_name='cn_data',
            region=REG_CN,
            expression_cache=None,
            calendar_cache=None
        )
        
        # 下载数据，默认下载最近3年数据
        start_time = '2025-01-01'
        end_time = '2025-01-31'
        
        logger.info(f"下载数据时间范围: {start_time} 到 {end_time}")
        D.calendar(start_time=start_time, end_time=end_time)
        
        # 验证数据
        instruments = D.list_instruments(instruments='all')
        logger.info(f"成功获取股票列表，共 {len(instruments)} 只股票")
        
        logger.info("Qlib数据初始化完成")
        return True
        
    except Exception as e:
        logger.error(f"初始化失败: {str(e)}")
        return False

if __name__ == "__main__":
    init_qlib_data()