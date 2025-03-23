import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from data.data_source.base import BaseDataSource
from data.data_source.qlib_source import QlibDataSource
from typing import List, Dict
import logging
import time
from utils.retry import retry_with_log

logger = logging.getLogger(__name__)

class MarketDataStorage:
    def __init__(self, db_path: str = 'data/market.db'):
        self.db_path = db_path
        self._init_db()
        
    def _init_db(self):
        """初始化数据库表结构"""
        with sqlite3.connect(self.db_path) as conn:
            # 创建股票基本信息表
            conn.execute('''
                CREATE TABLE IF NOT EXISTS stock_info (
                    ts_code TEXT PRIMARY KEY,
                    symbol TEXT,
                    name TEXT,
                    area TEXT,
                    industry TEXT,
                    list_date DATE,
                    market TEXT,
                    is_hs TEXT,
                    is_st BOOLEAN,
                    is_active BOOLEAN,
                    last_update DATE
                )
            ''')
            
            # 日线数据表
            conn.execute('''
                CREATE TABLE IF NOT EXISTS daily_price (
                    symbol TEXT,
                    trade_date DATE,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL,
                    amount REAL,
                    adj_factor REAL,
                    PRIMARY KEY (symbol, trade_date)
                )
            ''')
            
            # 分钟数据表
            conn.execute('''
                CREATE TABLE IF NOT EXISTS minute_price (
                    symbol TEXT,
                    time DATETIME,
                    freq TEXT,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL,
                    amount REAL,
                    PRIMARY KEY (symbol, time, freq)
                )
            ''')
            
            # 实时数据缓存表（内存表）
            conn.execute('''
                CREATE TABLE IF NOT EXISTS realtime_price (
                    symbol TEXT,
                    time DATETIME,
                    price REAL,
                    volume REAL,
                    amount REAL,
                    bid_price1 REAL,
                    ask_price1 REAL,
                    bid_volume1 REAL,
                    ask_volume1 REAL,
                    PRIMARY KEY (symbol, time)
                )
            ''')
            
            # 股票基本信息表
            conn.execute('''
                CREATE TABLE IF NOT EXISTS stock_basic (
                    symbol TEXT PRIMARY KEY,
                    name TEXT,
                    market TEXT,
                    list_date DATE,
                    delist_date DATE
                )
            ''')
            
            # 添加索引
            conn.execute('CREATE INDEX IF NOT EXISTS idx_industry ON stock_info(industry)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_is_active ON stock_info(is_active)')

            # 交易日历表
            conn.execute('''
                CREATE TABLE IF NOT EXISTS trade_calendar (
                    date DATE PRIMARY KEY,
                    is_open BOOLEAN,
                    prev_trade_date DATE
                )
            ''')
            
            # 行业分类表
            conn.execute('''
                CREATE TABLE IF NOT EXISTS industry_info (
                    symbol TEXT PRIMARY KEY,
                    industry_name TEXT,
                    industry_type TEXT,
                    update_date DATE
                )
            ''')
            
            # 财务数据表
            conn.execute('''
                CREATE TABLE IF NOT EXISTS financial_data (
                    symbol TEXT,
                    report_date DATE,
                    announce_date DATE,
                    total_assets REAL,
                    total_liab REAL,
                    total_revenue REAL,
                    net_income REAL,
                    roe REAL,
                    asset_turnover REAL,
                    current_ratio REAL,
                    PRIMARY KEY (symbol, report_date)
                )
            ''')

    @retry_with_log(tries=3, delay=2)
    def update_daily_data(self, data_source: BaseDataSource):
        """改进后的增量更新"""
        symbol_last_dates = self._get_symbol_last_dates()
        
        # 获取全量股票列表（复用Qlib/Tushare接口）
        symbols = self._get_all_symbols(data_source)  
        
        for symbol in symbols:
            # 个股增量逻辑
            last_date = symbol_last_dates.get(symbol)
            start_date = (last_date + timedelta(days=1)).strftime('%Y%m%d') if last_date else '19900101'
            end_date = datetime.now().strftime('%Y%m%d')
            
            # 带重试机制的数据获取（复用utils/retry）
            @retry_with_log(tries=3, delay=1)
            def fetch_data():
                return data_source.get_daily_data(symbol, start_date, end_date)
            
            df = fetch_data()
            if self._validate_data(df):
                self._save_daily_data(symbol, df)

    def update_minute_data(self, data_source, symbols: List[str], freq: str = '1min'):
        """更新分钟数据"""
        for symbol in symbols:
            df = data_source.get_min_data(symbol, freq)
            if df is not None and not df.empty:
                self._save_minute_data(symbol, df, freq)
                
    def update_realtime_data(self, data_source, symbols: List[str]):
        """更新实时数据"""
        for symbol in symbols:
            df = data_source.get_realtime_data(symbol)
            if df is not None and not df.empty:
                self._save_realtime_data(symbol, df) 

    def _save_daily_data(self, symbol: str, df: pd.DataFrame):
        """保存日线数据"""
        try:
            if not self._check_data_quality(df, 'daily'):
                logger.error(f"数据质量检查未通过: {symbol}")
                return
            
            # 数据清洗和格式转换
            df = self._clean_daily_data(df, symbol)
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 使用参数化查询
                insert_sql = """
                    INSERT OR REPLACE INTO daily_price 
                    (symbol, trade_date, open, high, low, close, volume, amount, adj_factor)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                # 准备数据
                data = [
                    (
                        symbol,
                        row['trade_date'].strftime('%Y%m%d'),
                        row['open'],
                        row['high'],
                        row['low'],
                        row['close'],
                        row['volume'],
                        row.get('amount', 0),
                        row.get('adj_factor', 1.0)
                    )
                    for _, row in df.iterrows()
                ]
                
                # 批量执行
                cursor.executemany(insert_sql, data)
                conn.commit()
                
                logger.info(f"成功保存{symbol}日线数据，共{len(data)}条")
            
        except Exception as e:
            logger.error(f"保存{symbol}日线数据失败: {str(e)}")
            raise
        
    def _save_minute_data(self, symbol: str, df: pd.DataFrame, freq: str):
        """保存分钟数据"""
        try:
            df = df.copy()
            
            # 确保必要的列存在
            required_columns = ['time', 'open', 'high', 'low', 'close', 'volume', 'amount']
            if not all(col in df.columns for col in required_columns):
                missing = [col for col in required_columns if col not in df.columns]
                raise ValueError(f"缺少必要的列: {missing}")
            
            # 添加额外信息
            df['freq'] = freq
            df['symbol'] = symbol
            
            with sqlite3.connect(self.db_path) as conn:
                # 先删除可能存在的重复数据
                conn.execute(
                    "DELETE FROM minute_price WHERE symbol = ? AND freq = ? AND time >= ? AND time <= ?",
                    (symbol, freq, df['time'].min(), df['time'].max())
                )
                
                # 保存新数据
                df.to_sql('minute_price', conn, if_exists='append', index=False)
                logger.info(f"保存{symbol}分钟数据成功，共{len(df)}条")
            
        except Exception as e:
            logger.error(f"保存{symbol}分钟数据失败: {str(e)}")
            raise
        
    def _save_realtime_data(self, symbol: str, df: pd.DataFrame):
        """保存实时数据到内存表"""
        with sqlite3.connect(self.db_path) as conn:
            # 先清除该股票的旧数据
            conn.execute(
                "DELETE FROM realtime_price WHERE symbol = ?", 
                (symbol,)
            )
            df.to_sql('realtime_price', conn, if_exists='append', index=False)

    def cleanup_old_data(self):
        """清理过期数据"""
        with sqlite3.connect(self.db_path) as conn:
            # 清理分钟数据
            conn.execute("""
                DELETE FROM minute_price 
                WHERE trade_time < datetime('now', '-7 days')
                AND freq = '1min'
            """)
            
            conn.execute("""
                DELETE FROM minute_price 
                WHERE trade_time < datetime('now', '-30 days')
                AND freq = '5min'
            """)
            
            # 清理实时数据
            conn.execute("""
                DELETE FROM realtime_price 
                WHERE time < datetime('now', '-1 day')
            """)
            
    def get_latest_price(self, symbol: str) -> dict:
        """获取最新价格"""
        with sqlite3.connect(self.db_path) as conn:
            # 先查实时数据
            df = pd.read_sql(
                "SELECT * FROM realtime_price WHERE symbol = ? ORDER BY time DESC LIMIT 1",
                conn,
                params=(symbol,)
            )
            
            if not df.empty:
                return df.iloc[0].to_dict()
            
            # 如果没有实时数据，查询最新日线数据
            df = pd.read_sql(
                "SELECT * FROM daily_price WHERE symbol = ? ORDER BY trade_date DESC LIMIT 1",
                conn,
                params=(symbol,)
            )
            
            if not df.empty:
                return df.iloc[0].to_dict()
            
            return None

    def get_kline_data(self, symbol: str, freq: str = '1min', limit: int = 100) -> pd.DataFrame:
        """获取K线数据
        freq: 1min, 5min, 15min, 30min, 60min, 1d
        """
        with sqlite3.connect(self.db_path) as conn:
            if freq == '1d':
                sql = """
                    SELECT * FROM daily_price 
                    WHERE symbol = ? 
                    ORDER BY trade_date DESC 
                    LIMIT ?
                """
            else:
                sql = """
                    SELECT * FROM minute_price 
                    WHERE symbol = ? AND freq = ?
                    ORDER BY time DESC 
                    LIMIT ?
                """
                params = (symbol, freq, limit)
            
            df = pd.read_sql(sql, conn, params=params)
            return df 

    def _get_connection(self) -> sqlite3.Connection:
        """获取数据库连接"""
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _check_data_quality(self, df: pd.DataFrame, data_type: str) -> bool:
        """检查数据质量
        data_type: daily, minute, realtime
        """
        if df.empty:
            return False
        
        # 检查必要字段
        required_fields = {
            'daily': ['open', 'high', 'low', 'close', 'volume'],
            'minute': ['open', 'high', 'low', 'close', 'volume'],
            'realtime': ['price', 'volume', 'bid_price1', 'ask_price1']
        }
        
        if not all(field in df.columns for field in required_fields[data_type]):
            logger.error(f"数据缺少必要字段: {required_fields[data_type]}")
            return False
        
        # 检查数值有效性
        numeric_fields = required_fields[data_type]
        for field in numeric_fields:
            if df[field].isnull().any():
                logger.warning(f"字段 {field} 存在空值")
            if (df[field] < 0).any():
                logger.error(f"字段 {field} 存在负值")
                return False
            
        return True

    def backup_database(self, backup_dir: str = 'data/backup'):
        """备份数据库"""
        import shutil
        import os
        from datetime import datetime
        
        os.makedirs(backup_dir, exist_ok=True)
        backup_file = os.path.join(
            backup_dir, 
            f"market_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        )
        
        with sqlite3.connect(self.db_path) as conn:
            # 确保所有数据写入磁盘
            conn.execute("PRAGMA wal_checkpoint(FULL)")
        
        shutil.copy2(self.db_path, backup_file)
        logger.info(f"数据库已备份到: {backup_file}")

    def update_stock_info(self, data_source):
        """更新股票基本信息"""
        try:
            df = data_source.get_stock_info()
            if df is not None and not df.empty:
                with sqlite3.connect(self.db_path) as conn:
                    df['last_update'] = datetime.now()
                    df.to_sql('stock_info', conn, if_exists='replace', index=False)
                    logger.info(f"更新股票信息成功，共 {len(df)} 只股票")
        except Exception as e:
            logger.error(f"更新股票信息失败: {str(e)}")

    def get_active_stocks(self, industry: str = None) -> List[str]:
        """获取活跃股票列表"""
        with sqlite3.connect(self.db_path) as conn:
            sql = "SELECT symbol FROM stock_info WHERE is_active = 1"
            if industry:
                sql += f" AND industry = ?"
                params = (industry,)
            else:
                params = ()
            
            df = pd.read_sql(sql, conn, params=params)
            return df['symbol'].tolist()

    def update_trade_calendar(self, data_source, start_date: str, end_date: str):
        """更新交易日历"""
        df = data_source.get_trade_calendar(start_date, end_date)
        if df is not None and not df.empty:
            with sqlite3.connect(self.db_path) as conn:
                df.to_sql('trade_calendar', conn, if_exists='replace', index=True)

    def update_industry_info(self, data_source: BaseDataSource):
        """更新行业分类信息"""
        df = data_source.get_industry_info()
        if df is not None and not df.empty:
            with sqlite3.connect(self.db_path) as conn:
                # 清空旧数据
                conn.execute("DELETE FROM industry_info")
                # 验证必要字段存在
                required_columns = ['symbol', 'industry_name', 'industry_type']
                if not all(col in df.columns for col in required_columns):
                    missing = [col for col in required_columns if col not in df.columns]
                    raise KeyError(f"行业数据缺少必要字段: {missing}")
                # 插入新数据
                df[required_columns].to_sql(
                    'industry_info', 
                    conn, 
                    if_exists='append', 
                    index=False
                )

    def update_financial_data(self, data_source, symbol: str, start_date: str, end_date: str):
        """更新财务数据"""
        df = data_source.get_financial_data(symbol, start_date, end_date)
        if df is not None and not df.empty:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                insert_sql = """
                    INSERT OR REPLACE INTO financial_data 
                    (symbol, report_date, announce_date, total_assets, total_liab, 
                     total_revenue, net_income, roe, asset_turnover, current_ratio)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                data = [
                    (
                        row['symbol'],
                        row['report_date'],
                        row['announce_date'],
                        row['total_assets'],
                        row['total_liab'],
                        row['total_revenue'],
                        row['net_income'],
                        row['roe'],
                        row['asset_turnover'],
                        row['current_ratio']
                    )
                    for _, row in df.iterrows()
                ]
                cursor.executemany(insert_sql, data)
                conn.commit()

    def get_financial_indicators(self, symbol: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """获取财务指标"""
        with self._get_connection() as conn:
            sql = """
                SELECT * FROM financial_data 
                WHERE ts_code = ?
            """
            params = [symbol]
            
            if start_date:
                sql += " AND report_date >= ?"
                params.append(start_date)
            if end_date:
                sql += " AND report_date <= ?"
                params.append(end_date)
            
            sql += " ORDER BY report_date DESC"
            return pd.read_sql(sql, conn, params=params)

    def get_industry_stocks(self, industry_name: str) -> List[str]:
        """获取行业成分股"""
        with self._get_connection() as conn:
            sql = """
                SELECT symbol FROM industry_info 
                WHERE industry_name = ?
            """
            df = pd.read_sql(sql, conn, params=(industry_name,))
            return df['symbol'].tolist()

    def get_trading_dates(self, start_date: str, end_date: str) -> List[str]:
        """获取交易日期列表"""
        with self._get_connection() as conn:
            sql = """
                SELECT date FROM trade_calendar 
                WHERE date BETWEEN ? AND ?
                AND is_open = 1
                ORDER BY date
            """
            df = pd.read_sql(sql, conn, params=(start_date, end_date))
            return df['date'].tolist()

    def check_data_consistency(self) -> Dict:
        """检查数据一致性"""
        with self._get_connection() as conn:
            results = {}
            
            # 检查交易日历完整性
            sql = """
                SELECT MIN(date) as start_date, 
                       MAX(date) as end_date,
                       COUNT(*) as total_days,
                       SUM(CASE WHEN is_open = 1 THEN 1 ELSE 0 END) as trading_days
                FROM trade_calendar
            """
            df = pd.read_sql(sql, conn)
            results['calendar'] = df.iloc[0].to_dict()
            
            # 检查每只股票的数据完整性
            sql = """
                SELECT ts_code as symbol,
                       COUNT(DISTINCT trade_date) as data_days,
                       MIN(trade_date) as first_date,
                       MAX(trade_date) as last_date
                FROM daily_price
                GROUP BY ts_code
            """
            df = pd.read_sql(sql, conn)
            results['daily_data'] = df.to_dict('records')
            
            # 检查财务数据完整性
            sql = """
                SELECT ts_code as symbol,
                       COUNT(DISTINCT report_date) as report_count,
                       MIN(report_date) as first_report,
                       MAX(report_date) as last_report
                FROM financial_data
                GROUP BY ts_code
            """
            df = pd.read_sql(sql, conn)
            results['financial_data'] = df.to_dict('records')
            
            return results 

    def restore_from_backup(self, backup_file: str):
        """从备份恢复数据"""
        import shutil
        import os
        
        if not os.path.exists(backup_file):
            raise FileNotFoundError(f"备份文件不存在: {backup_file}")
        
        # 先备份当前数据库
        current_backup = self.backup_database()
        
        try:
            # 关闭所有连接
            with self._get_connection() as conn:
                conn.close()
            
            # 恢复数据
            shutil.copy2(backup_file, self.db_path)
            logger.info(f"数据已从 {backup_file} 恢复")
            
        except Exception as e:
            # 恢复失败时还原之前的数据
            shutil.copy2(current_backup, self.db_path)
            logger.error(f"数据恢复失败: {str(e)}")
            raise 

    def close_all_connections(self):
        """关闭所有数据库连接"""
        try:
            conn = None
            try:
                conn = self._get_connection()
                # 强制提交所有更改
                conn.commit()
                # 关闭WAL模式
                conn.execute("PRAGMA wal_checkpoint(FULL)")
                # 切换到普通模式
                conn.execute("PRAGMA journal_mode=DELETE")
            finally:
                if conn:
                    try:
                        conn.close()
                    except:
                        pass
                    
            # 等待文件释放
            time.sleep(1)  # 增加等待时间
        except Exception as e:
            logger.warning(f"关闭数据库连接时发生错误: {str(e)}") 

    def _get_symbol_last_dates(self) -> Dict[str, datetime]:
        """获取各股票最后更新日期"""
        with self._get_connection() as conn:
            df = pd.read_sql(
                "SELECT ts_code as symbol, MAX(trade_date) as last_date FROM daily_price GROUP BY ts_code",
                conn
            )
        return df.set_index('symbol')['last_date'].dt.date.to_dict()

    def _get_all_symbols(self, data_source: BaseDataSource) -> List[str]:
        # Implementation of _get_all_symbols method
        # This method should return a list of all symbols available in the data source
        # This is a placeholder and should be implemented based on the actual data source
        return []

    def _validate_data(self, df: pd.DataFrame) -> bool:
        """增强型数据校验"""
        checks = [
            (df['close'] > 0, '收盘价必须为正数'),
            (df['volume'] >= 0, '成交量不能为负'),
            (df['high'] >= df['low'], '最高价不能低于最低价'),
            (df.groupby('trade_date').size() == 1, '存在重复日期数据')
        ]
        
        for condition, msg in checks:
            if not condition.all():
                logger.error(f"数据校验失败: {msg}")
                return False
        return True

    def _get_last_update_date(self) -> datetime:
        # Implementation of _get_last_update_date method
        # This method should return the last update date from the daily_price table
        # This is a placeholder and should be implemented based on the actual implementation
        return datetime.fromtimestamp(0)

    def _clean_daily_data(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """数据清洗"""
        # 转换日期格式
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        
        # 去除重复数据（保留最新）
        df = df.drop_duplicates(subset=['trade_date'], keep='last')
        
        # 验证价格有效性
        df = df[
            (df['close'] > 0) & 
            (df['volume'] > 0) &
            (df['high'] >= df['low'])
        ]
        
        logger.debug(f"{symbol}清洗后数据量: {len(df)}")
        return df

    def _get_last_update_date(self) -> datetime:
        # Implementation of _get_last_update_date method
        # This method should return the last update date from the daily_price table
        # This is a placeholder and should be implemented based on the actual implementation
        return datetime.fromtimestamp(0)

    def _get_last_update_date(self) -> datetime:
        # Implementation of _get_last_update_date method
        # This method should return the last update date from the daily_price table
        # This is a placeholder and should be implemented based on the actual implementation
        return datetime.fromtimestamp(0) 