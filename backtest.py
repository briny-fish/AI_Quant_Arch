from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from datetime import datetime
from strategies.base_strategy import BaseStrategy

class BacktestEngine:
    def __init__(self, 
                 data_source,
                 initial_capital: float = 1000000.0,
                 commission_rate: float = 0.0003):
        self.data_source = data_source
        self.initial_capital = initial_capital
        self.commission_rate = commission_rate
        self.positions: Dict[str, int] = {}
        self.trades: List[Dict] = []
        self.daily_stats: List[Dict] = []  # 每日统计数据
        
    def run(self, 
            strategy: BaseStrategy,
            start_date: str,
            end_date: str,
            symbols: List[str]) -> Dict:
        """运行回测"""
        # 初始化策略
        strategy.cash = self.initial_capital
        strategy.initialize()
        
        # 获取回测数据
        data = self._prepare_data(symbols, start_date, end_date)
        
        # 按时间顺序遍历数据
        for date, bars in data.groupby(level=0):
            # 更新策略当前时间
            strategy.current_time = date
            
            # 更新持仓市值
            self._update_positions_value(strategy, bars)
            
            # 记录每日统计数据
            self._record_daily_stats(strategy, date)
            
            # 运行策略
            strategy.on_bar(bars)
            
        # 计算回测结果
        return self._calculate_results()
        
    def _prepare_data(self, symbols: List[str], start_date: str, end_date: str) -> pd.DataFrame:
        """准备回测数据"""
        all_data = []
        for symbol in symbols:
            df = self.data_source.get_daily_data(symbol, start_date, end_date)
            if df is not None and not df.empty:
                df['symbol'] = symbol
                all_data.append(df)
        
        if not all_data:
            raise ValueError("没有获取到任何数据")
        
        # 合并所有股票数据
        data = pd.concat(all_data)
        # 重要：确保索引是日期，并且按日期排序
        if not isinstance(data.index, pd.DatetimeIndex):
            data['trade_date'] = pd.to_datetime(data['trade_date'])
            data.set_index('trade_date', inplace=True)
        data.sort_index(inplace=True)
        print(f"获取到 {len(data)} 条数据记录")
        print(f"数据字段: {data.columns.tolist()}")
        return data
        
    def _update_positions_value(self, strategy: BaseStrategy, bars: pd.DataFrame):
        """更新持仓市值"""
        for symbol, quantity in strategy.positions.items():
            symbol_data = bars[bars['symbol'] == symbol]
            if not symbol_data.empty:
                current_price = symbol_data['close'].iloc[0]
                strategy.positions_value[symbol] = quantity * current_price
                
    def _record_daily_stats(self, 
                           strategy: BaseStrategy, 
                           date: datetime):
        """记录每日统计数据"""
        stats = {
            'date': pd.to_datetime(date),  # 确保日期是datetime类型
            'cash': strategy.cash,
            'positions_value': strategy.get_positions_value(),
            'total_value': strategy.get_total_value()
        }
        self.daily_stats.append(stats)
        
    def _calculate_results(self) -> Dict:
        """计算回测指标"""
        df = pd.DataFrame(self.daily_stats)
        
        # 将日期字符串转换为datetime类型
        df['date'] = pd.to_datetime(df['date'])
        
        # 计算每日收益率
        df['returns'] = df['total_value'].pct_change()
        df['cum_returns'] = (1 + df['returns']).cumprod()
        
        # 计算基础指标
        total_return = (df['total_value'].iloc[-1] - self.initial_capital) / self.initial_capital
        days = (df['date'].iloc[-1] - df['date'].iloc[0]).days
        annual_return = (1 + total_return) ** (365 / days) - 1
        
        # 计算风险指标
        risk_free_rate = 0.03  # 假设无风险利率为3%
        excess_returns = df['returns'] - risk_free_rate / 252
        sharpe_ratio = np.sqrt(252) * excess_returns.mean() / excess_returns.std()
        
        # 计算回撤
        df['cum_max'] = df['cum_returns'].cummax()
        df['drawdown'] = (df['cum_max'] - df['cum_returns']) / df['cum_max']
        max_drawdown = df['drawdown'].max()
        
        # 计算交易统计
        trades_df = pd.DataFrame(self.trades)
        if not trades_df.empty:
            win_trades = trades_df[trades_df['revenue'] > trades_df['cost']].shape[0] if 'revenue' in trades_df else 0
            total_trades = len(trades_df)
            win_rate = win_trades / total_trades if total_trades > 0 else 0
        else:
            win_rate = 0
            total_trades = 0
        
        return {
            'total_return': total_return,
            'annual_return': annual_return,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'win_rate': win_rate,
            'total_trades': total_trades,
            'trades': self.trades,
            'daily_stats': df.to_dict('records'),
            'positions_history': self.positions
        }

class Backtest:
    def __init__(self, data_source):
        self.engine = BacktestEngine(data_source)
        
    def run(self, 
            strategy_class,
            symbols: List[str],
            start_date: str,
            end_date: str,
            strategy_params: Dict = None) -> Dict:
        """运行回测"""
        print(f"开始回测: {start_date} 到 {end_date}")
        print(f"股票池: {symbols}")
        
        # 初始化策略
        strategy = strategy_class(self.engine.data_source, strategy_params)
        strategy.cash = self.engine.initial_capital
        strategy.initialize()
        
        # 获取回测数据
        data = self.engine._prepare_data(symbols, start_date, end_date)
        
        # 按时间顺序遍历数据
        for date, bars in data.groupby(level=0):
            # 更新策略当前时间
            strategy.current_time = date
            
            # 更新持仓市值
            self.engine._update_positions_value(strategy, bars)
            
            # 记录每日统计数据
            self.engine._record_daily_stats(strategy, date)
            
            # 运行策略
            strategy.on_bar(bars)
            
        # 计算回测结果
        return self.engine._calculate_results()
        
    def _prepare_data(self, symbols: List[str], 
                     start_date: str, 
                     end_date: str) -> pd.DataFrame:
        """准备回测数据"""
        all_data = []
        for symbol in symbols:
            df = self.engine.data_source.get_daily_data(symbol, start_date, end_date)
            if df is not None and not df.empty:
                df['symbol'] = symbol
                all_data.append(df)
        
        if not all_data:
            raise ValueError("没有获取到任何数据")
        
        # 合并所有股票数据
        data = pd.concat({sym: df for sym, df in zip(symbols, all_data)}, 
                    axis=0)
        data.set_index('trade_date', inplace=True)
        data.sort_index(inplace=True)
        print(f"获取到 {len(data)} 条数据记录")
        return data
        
    def _update_positions_value(self, 
                              strategy: BaseStrategy, 
                              bars: pd.DataFrame):
        """更新持仓市值"""
        for symbol, quantity in strategy.positions.items():
            symbol_data = bars[bars['symbol'] == symbol]
            if not symbol_data.empty:
                current_price = symbol_data['close'].iloc[0]
                strategy.positions_value[symbol] = quantity * current_price
                
    def _record_daily_stats(self, 
                           strategy: BaseStrategy, 
                           date: datetime):
        """记录每日统计数据"""
        stats = {
            'date': pd.to_datetime(date),  # 确保日期是datetime类型
            'cash': strategy.cash,
            'positions_value': strategy.get_positions_value(),
            'total_value': strategy.get_total_value()
        }
        self.engine.daily_stats.append(stats)
        
    def _calculate_results(self) -> Dict:
        """计算回测指标"""
        df = pd.DataFrame(self.engine.daily_stats)
        
        # 将日期字符串转换为datetime类型
        df['date'] = pd.to_datetime(df['date'])
        
        # 计算每日收益率
        df['returns'] = df['total_value'].pct_change()
        df['cum_returns'] = (1 + df['returns']).cumprod()
        
        # 计算基础指标
        total_return = (df['total_value'].iloc[-1] - self.engine.initial_capital) / self.engine.initial_capital
        days = (df['date'].iloc[-1] - df['date'].iloc[0]).days
        annual_return = (1 + total_return) ** (365 / days) - 1
        
        # 计算风险指标
        risk_free_rate = 0.03  # 假设无风险利率为3%
        excess_returns = df['returns'] - risk_free_rate / 252
        sharpe_ratio = np.sqrt(252) * excess_returns.mean() / excess_returns.std()
        
        # 计算回撤
        df['cum_max'] = df['cum_returns'].cummax()
        df['drawdown'] = (df['cum_max'] - df['cum_returns']) / df['cum_max']
        max_drawdown = df['drawdown'].max()
        
        # 计算交易统计
        trades_df = pd.DataFrame(self.engine.trades)
        if not trades_df.empty:
            win_trades = trades_df[trades_df['revenue'] > trades_df['cost']].shape[0] if 'revenue' in trades_df else 0
            total_trades = len(trades_df)
            win_rate = win_trades / total_trades if total_trades > 0 else 0
        else:
            win_rate = 0
            total_trades = 0
        
        return {
            'total_return': total_return,
            'annual_return': annual_return,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'win_rate': win_rate,
            'total_trades': total_trades,
            'trades': self.engine.trades,
            'daily_stats': df.to_dict('records'),
            'positions_history': self.engine.positions
        } 