class BaseConfig:
    """基础配置"""
    DATA_START_YEAR = 2005      # 数据起始年份
    MAX_API_CALLS = 500         # 每日最大API调用次数
    API_CALL_INTERVAL = 1.0     # API调用间隔（秒）

class TestConfig(BaseConfig):
    """测试环境配置"""
    ENABLE_MINUTE_TEST = False
    MAX_API_CALLS = 50      # 测试环境降低调用限制

# 当前使用的配置实例
current_config = TestConfig()  # 测试时使用测试配置
# current_config = BaseConfig()  # 生产时切换 