import os
import sys
from datetime import datetime, timedelta
from typing import Dict

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.data_source.tushare_api import TushareAPI
from strategies.factor_strategy import SimpleFactorStrategy
from backtest import Backtest

def main():
    # 初始化数据源
    api_key = "4da2fbec9c2cee373d3aace9f9e200a315a2812dc11267c425010cec"  # 替换为你的token
    data_source = TushareAPI(api_key)
    
    # 设置回测参数
    symbols = ['000001.SZ', '600000.SH', '600036.SH']  # 测试用的股票池
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
    
    # 策略参数
    strategy_params = {
        'lookback_period': 20,      # 动量回看期
        'holding_period': 5,        # 持仓周期
        'position_size': 0.3,        # 单个股票持仓比例
        'initial_capital': 1000000,  # 初始资金100万
        'symbols': symbols,          # 股票池
        'start_date': start_date,    # 开始日期
        'end_date': end_date         # 结束日期
    }
    
    # 创建回测实例
    backtest = Backtest(data_source)
    
    # 运行回测
    results = backtest.run(
        strategy_class=SimpleFactorStrategy,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        strategy_params=strategy_params
    )
    
    # 打印回测结果
    print("\n=== 回测结果 ===")
    print(f"总收益率: {results['total_return']:.2%}")
    print(f"年化收益率: {results['annual_return']:.2%}")
    print(f"夏普比率: {results['sharpe_ratio']:.2f}")
    print(f"最大回撤: {results['max_drawdown']:.2%}")
    
if __name__ == "__main__":
    main() 