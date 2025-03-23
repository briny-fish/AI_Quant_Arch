import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List

from data.data_source.tushare_api import TushareAPI
from strategies.factor_strategy import SimpleFactorStrategy
from backtest import Backtest

class Dashboard:
    def __init__(self):
        # 初始化数据源
        api_key = st.secrets["tushare_api_key"]  # 从 Streamlit secrets 获取
        self.data_source = TushareAPI(api_key)
        
    def run(self):
        st.title("量化交易回测系统")
        
        # 侧边栏 - 参数设置
        with st.sidebar:
            self._render_sidebar()
            
        # 主界面
        tab1, tab2, tab3 = st.tabs(["回测结果", "交易记录", "持仓分析"])
        
        with tab1:
            self._render_backtest_results()
            
        with tab2:
            self._render_trades()
            
        with tab3:
            self._render_positions()
            
    def _render_sidebar(self):
        """渲染侧边栏"""
        st.header("策略参数")
        
        # 股票池设置
        st.subheader("股票池")
        st.caption("请输入股票代码，格式如：000001.SZ（股票代码+市场代码）")
        symbols = st.text_input(
            "股票代码（用逗号分隔）",
            value="000001.SZ,600000.SH,600036.SH"
        ).split(',')
        
        # 日期范围
        st.subheader("回测区间")
        end_date = st.date_input(
            "结束日期",
            value=datetime.now()
        )
        start_date = st.date_input(
            "开始日期",
            value=end_date - timedelta(days=365)
        )
        
        # 策略参数
        st.subheader("策略参数")
        st.caption("动量回看期：计算动量信号的历史周期（单位：天）")
        lookback_period = st.slider("动量回看期（天）", 5, 60, 20)
        
        st.caption("持仓周期：持有股票的最短时间（单位：天）")
        holding_period = st.slider("持仓周期（天）", 1, 20, 5)
        
        st.caption("单股仓位：每只股票的最大持仓比例（占总资金的百分比）")
        position_size = st.slider("单股仓位（%）", 10, 100, 30) / 100
        
        # 运行回测按钮
        if st.button("运行回测"):
            with st.spinner('正在运行回测...'):
                strategy_params = {
                    'lookback_period': lookback_period,
                    'holding_period': holding_period,
                    'position_size': position_size,
                    'symbols': symbols,
                    'start_date': start_date.strftime('%Y%m%d'),
                    'end_date': end_date.strftime('%Y%m%d')
                }
                
                # 运行回测
                backtest = Backtest(self.data_source)
                results = backtest.run(
                    strategy_class=SimpleFactorStrategy,
                    symbols=symbols,
                    start_date=start_date.strftime('%Y%m%d'),
                    end_date=end_date.strftime('%Y%m%d'),
                    strategy_params=strategy_params
                )
                
                # 保存结果到 session state
                st.session_state['backtest_results'] = results
                st.success('回测完成！')
        
    def _render_backtest_results(self):
        """渲染回测结果"""
        if 'backtest_results' not in st.session_state:
            st.info("请在左侧设置参数并运行回测")
            return
            
        results = st.session_state['backtest_results']
        
        # 显示主要指标
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("总收益率", f"{results['total_return']:.2%}")
        with col2:
            st.metric("年化收益率", f"{results['annual_return']:.2%}")
        with col3:
            st.metric("夏普比率", f"{results['sharpe_ratio']:.2f}")
        with col4:
            st.metric("最大回撤", f"{results['max_drawdown']:.2%}")
            
        # 显示交易统计
        col5, col6 = st.columns(2)
        with col5:
            st.metric("胜率", f"{results['win_rate']:.2%}")
        with col6:
            st.metric("总交易次数", f"{results['total_trades']}")
        
        # 绘制净值曲线
        df = pd.DataFrame(results['daily_stats'])
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['total_value'],
            mode='lines',
            name='策略净值'
        ))
        
        # 添加回撤阴影
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['drawdown'],
            fill='tozeroy',
            name='回撤',
            yaxis='y2'
        ))
        
        fig.update_layout(
            title="策略净值与回撤",
            xaxis_title="日期",
            yaxis_title="净值",
            yaxis2=dict(
                title="回撤",
                overlaying='y',
                side='right'
            ),
            height=500
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
    def _render_trades(self):
        """渲染交易记录"""
        if 'backtest_results' not in st.session_state:
            st.info("请先运行回测")
            return
            
        trades = pd.DataFrame(st.session_state['backtest_results']['trades'])
        if not trades.empty:
            trades['time'] = pd.to_datetime(trades['time'])
            trades['收益'] = trades.apply(
                lambda x: x['revenue'] - x['cost'] if 'revenue' in x else 0, 
                axis=1
            )
            
            # 显示交易统计
            col1, col2 = st.columns(2)
            with col1:
                st.metric("总交易次数", len(trades))
            with col2:
                st.metric("平均每笔收益", f"{trades['收益'].mean():.2f}")
            
            # 显示交易记录
            st.dataframe(trades)
            
            # 绘制交易收益分布
            fig = px.histogram(
                trades,
                x='收益',
                title="交易收益分布"
            )
            st.plotly_chart(fig, use_container_width=True)
            
    def _render_positions(self):
        """渲染持仓分析"""
        if 'backtest_results' not in st.session_state:
            st.info("请先运行回测")
            return
            
        daily_stats = pd.DataFrame(st.session_state['backtest_results']['daily_stats'])
        if not daily_stats.empty:
            # 绘制持仓市值占比
            fig = px.pie(
                daily_stats.iloc[-1:],
                values=['positions_value', 'cash'],
                names=['持仓', '现金'],
                title="当前资产配置"
            )
            st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    dashboard = Dashboard()
    dashboard.run() 