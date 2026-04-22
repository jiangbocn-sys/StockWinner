"""
股票基本信息更新服务

功能：
1. 从SDK获取当前上市股票的基本信息
2. 合并股本数据、行业分类数据
3. 每日更新一次到stock_base_info表

数据来源：
- get_code_info: 股票名称、昨收价、涨跌停价、上市日期
- get_equity_structure: 总股本、流通股本
- get_industry_base_info: 申万行业分类
"""

import sqlite3
import pandas as pd
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Dict, Optional
import asyncio

from services.common.sdk_manager import get_sdk_manager
from services.common.sdk_column_mapping import (
    map_code_info_columns,
    map_equity_columns,
    map_industry_columns,
)
from services.common.timezone import CHINA_TZ, get_china_time

DB_PATH = Path(__file__).parent.parent.parent / "data" / "kline.db"


class StockBaseInfoService:
    """股票基本信息服务"""

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.sdk_manager = get_sdk_manager()

    def _get_connection(self) -> sqlite3.Connection:
        """获取数据库连接"""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def _init_table(self):
        """初始化表（确保表存在）"""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stock_base_info (
                stock_code TEXT PRIMARY KEY,
                stock_name TEXT,
                market TEXT,
                security_status TEXT,
                pre_close REAL,
                high_limited REAL,
                low_limited REAL,
                price_tick REAL,
                list_date DATE,
                delist_date DATE,
                sw_level1 TEXT,
                sw_level2 TEXT,
                sw_level3 TEXT,
                total_share REAL,
                float_share REAL,
                source TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
        ''')

        conn.commit()
        conn.close()
        print("[StockBaseInfo] 表初始化完成")

    def get_stock_list(self) -> List[str]:
        """获取当前上市A股股票代码列表"""
        try:
            codes = self.sdk_manager.get_code_list(security_type='EXTRA_STOCK_A')
            return codes
        except Exception as e:
            print(f"[StockBaseInfo] 获取股票列表失败: {e}")
            # 从数据库获取已有股票代码作为备选
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT stock_code FROM kline_data")
            codes = [row[0] for row in cursor.fetchall()]
            conn.close()
            return codes

    def fetch_code_info(self) -> pd.DataFrame:
        """从SDK获取股票基本信息"""
        print("[StockBaseInfo] 正在获取股票基本信息...")
        df = self.sdk_manager.get_code_info(security_type='EXTRA_STOCK_A')

        if df.empty:
            print("[StockBaseInfo] 未获取到股票基本信息")
            return pd.DataFrame()

        # get_code_info返回的DataFrame以code_market为索引
        # 需要将索引转换为列
        if df.index.name == 'code_market' or 'code_market' not in df.columns:
            df['stock_code'] = df.index

        # 列名映射
        df = map_code_info_columns(df)

        # 从股票代码提取市场
        df['market'] = df['stock_code'].apply(lambda x: x.split('.')[1] if '.' in str(x) else 'UNKNOWN')

        print(f"[StockBaseInfo] 获取到 {len(df)} 条股票基本信息")
        return df

    def fetch_equity_structure(self, stock_codes: List[str], batch_size: int = 100) -> pd.DataFrame:
        """从SDK获取股本结构数据"""
        print(f"[StockBaseInfo] 正在获取股本数据 ({len(stock_codes)} 只股票)...")

        all_data = []
        for i in range(0, len(stock_codes), batch_size):
            batch = stock_codes[i:i + batch_size]
            print(f"  批次 {i//batch_size + 1}: {batch[0]} ~ {batch[-1]}")

            try:
                df = self.sdk_manager.get_equity_structure(batch)
                if not df.empty:
                    df = map_equity_columns(df)
                    all_data.append(df)
            except Exception as e:
                print(f"  批次获取失败: {e}")

        if all_data:
            result = pd.concat(all_data, ignore_index=True)
            # 取每只股票最新的股本数据
            if 'ann_date' in result.columns:
                result = result.sort_values('ann_date').groupby('stock_code').last().reset_index()
            print(f"[StockBaseInfo] 获取到 {len(result)} 条股本数据")
            return result

        return pd.DataFrame()

    def fetch_industry_info(self) -> pd.DataFrame:
        """从SDK获取行业分类信息"""
        print("[StockBaseInfo] 正在获取行业分类数据...")
        df = self.sdk_manager.get_industry_base_info()

        if df.empty:
            print("[StockBaseInfo] 未获取到行业分类数据")
            return pd.DataFrame()

        df = map_industry_columns(df)
        print(f"[StockBaseInfo] 获取到 {len(df)} 条行业分类数据")
        return df

    def merge_and_save(self, code_info: pd.DataFrame, equity: pd.DataFrame, industry: pd.DataFrame) -> int:
        """合并数据并保存到数据库"""
        print("[StockBaseInfo] 正在合并数据...")

        if code_info.empty:
            print("[StockBaseInfo] 无基本信息数据，跳过保存")
            return 0

        # 以code_info为基础
        merged = code_info.copy()

        # 合并股本数据
        if not equity.empty and 'stock_code' in equity.columns:
            equity_cols = ['stock_code', 'total_share', 'float_share']
            equity_subset = equity[equity_cols].drop_duplicates(subset=['stock_code'])
            merged = merged.merge(equity_subset, on='stock_code', how='left')

        # 行业分类暂不合并（SDK返回的是指数成分数据，需另外获取股票-行业映射）
        # 可通过其他方式获取：如申万行业分类API

        # 准备保存
        current_time = get_china_time().isoformat()
        merged['source'] = 'sdk_sync'
        merged['created_at'] = current_time
        merged['updated_at'] = current_time

        # 保存到数据库
        conn = self._get_connection()
        cursor = conn.cursor()

        saved = 0
        for _, row in merged.iterrows():
            try:
                cursor.execute('''
                    INSERT OR REPLACE INTO stock_base_info (
                        stock_code, stock_name, market, security_status,
                        pre_close, high_limited, low_limited, price_tick,
                        list_date, delist_date,
                        sw_level1, sw_level2, sw_level3,
                        total_share, float_share,
                        source, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    row.get('stock_code'),
                    row.get('stock_name'),
                    row.get('market'),
                    row.get('security_status'),
                    row.get('pre_close'),
                    row.get('high_limited'),
                    row.get('low_limited'),
                    row.get('price_tick'),
                    row.get('list_date'),
                    row.get('delist_date'),
                    row.get('sw_level1'),
                    row.get('sw_level2'),
                    row.get('sw_level3'),
                    row.get('total_share'),
                    row.get('float_share'),
                    row.get('source'),
                    row.get('created_at'),
                    row.get('updated_at'),
                ))
                saved += 1

                if saved % 500 == 0:
                    conn.commit()
                    print(f"  已保存 {saved} 条记录")

            except Exception as e:
                print(f"  保存 {row.get('stock_code')} 失败: {e}")

        conn.commit()
        conn.close()
        print(f"[StockBaseInfo] 保存完成，共 {saved} 条记录")

        return saved

    def update_all(self) -> Dict:
        """执行完整更新流程"""
        print("=" * 60)
        print("股票基本信息更新")
        print("=" * 60)

        # 确保表存在
        self._init_table()

        # 获取股票列表
        stock_codes = self.get_stock_list()
        print(f"[StockBaseInfo] 股票列表: {len(stock_codes)} 只")

        # 获取基本信息
        code_info = self.fetch_code_info()

        # 获取股本数据（分批获取全部股票）
        equity = self.fetch_equity_structure(stock_codes)

        # 获取行业分类并合并
        industry = self.fetch_industry_info()

        # 合并并保存
        saved = self.merge_and_save(code_info, equity, industry)

        print("=" * 60)
        print(f"[StockBaseInfo] 更新完成，保存 {saved} 条记录")
        print("=" * 60)

        return {
            'status': 'success',
            'stock_count': len(stock_codes),
            'code_info_count': len(code_info),
            'equity_count': len(equity),
            'saved_count': saved,
        }

    async def update_all_async(self) -> Dict:
        """异步版本：执行完整更新流程"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.update_all)


# ==================== CLI 入口 ====================

def run_update_stock_base_info():
    """运行股票基本信息更新"""
    service = StockBaseInfoService()
    result = service.update_all()
    return result


if __name__ == "__main__":
    run_update_stock_base_info()