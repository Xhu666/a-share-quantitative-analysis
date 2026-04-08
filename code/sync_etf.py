import akshare as ak
import baostock as bs
import pandas as pd
import sqlite3
import os
import time
import numpy as np
from datetime import datetime, timedelta
from tqdm import tqdm

# --- 1. 自动化路径配置 ---
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
db_dir = os.path.join(root_dir, "data")
if not os.path.exists(db_dir):
    os.makedirs(db_dir)

DB_PATH = os.path.join(db_dir, "stock_data.db")

# --- 2. 时间配置 ---
DEFAULT_START = (datetime.now() - timedelta(days=5 * 365)).strftime("%Y-%m-%d")
END_DATE = datetime.now().strftime("%Y-%m-%d")

CORE_INDICES = {
    "sh.000300": "沪深300",
    "sh.000905": "中证500",
    "sz.399006": "创业板指",
    "sh.000001": "上证指数"
}


def init_tables():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS etf_info (code TEXT PRIMARY KEY, code_name TEXT)')
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS etf_daily
                   (
                       date
                       TEXT,
                       code
                       TEXT,
                       open
                       REAL,
                       high
                       REAL,
                       low
                       REAL,
                       close
                       REAL,
                       preclose
                       REAL,
                       volume
                       INTEGER,
                       amount
                       REAL,
                       turn
                       REAL,
                       pctChg
                       REAL,
                       PRIMARY
                       KEY
                   (
                       date,
                       code
                   )
                       )
                   ''')
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS index_daily
                   (
                       date
                       TEXT,
                       code
                       TEXT,
                       close
                       REAL,
                       pctChg
                       REAL,
                       PRIMARY
                       KEY
                   (
                       date,
                       code
                   )
                       )
                   ''')
    conn.commit()
    conn.close()


def get_etf_list():
    bs.login()
    all_stock = pd.DataFrame()
    for i in range(0, 35):
        target_day = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
        rs = bs.query_all_stock(day=target_day)
        all_stock = rs.get_data()
        if not all_stock.empty and 'code' in all_stock.columns:
            print(f"✅ 成功获取 {target_day} 的全市场列表。")
            break
    bs.logout()
    if all_stock.empty: return pd.DataFrame()
    return all_stock[all_stock['code'].str.match(r'^sh\.5|^sz\.15|^sz\.16|^sz\.18')].copy()


def sync_data():
    init_tables()
    etf_list_df = get_etf_list()
    if etf_list_df.empty:
        print("❌ 无法同步名单，请检查网络。")
        return

    conn = sqlite3.connect(DB_PATH)
    etf_list_df[['code', 'code_name']].to_sql("etf_info", conn, if_exists="replace", index=False)
    etf_codes = etf_list_df['code'].tolist()

    last_update_map = {}
    try:
        df_old = pd.read_sql("SELECT code, MAX(date) as last_date FROM etf_daily GROUP BY code", conn)
        last_update_map = dict(zip(df_old['code'], df_old['last_date']))
    except:
        pass

    # --- 步骤 1: 确定最新交易日 (BaoStock) ---
    print(f"\n[1/2] 正在确定市场状态并同步指数...")
    bs.login()
    latest_market_date = "1990-01-01"
    for i in range(0, 35):
        check_day = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
        rs_check = bs.query_history_k_data_plus("sh.000001", "date", start_date=check_day, end_date=check_day,
                                                frequency="d")
        res_df = rs_check.get_data()
        if not res_df.empty and 'date' in res_df.columns:
            latest_market_date = res_df['date'].max()
            print(f"📈 市场最新交易日: {latest_market_date}")
            break

    # 同步指数
    for idx_code, idx_name in CORE_INDICES.items():
        rs = bs.query_history_k_data_plus(idx_code, "date,code,close,pctChg",
                                          start_date=DEFAULT_START, end_date=END_DATE, frequency="d")
        df = rs.get_data()
        if not df.empty and 'date' in df.columns:
            df[['close', 'pctChg']] = df[['close', 'pctChg']].apply(pd.to_numeric, errors='coerce')
            df.to_sql("temp_index", conn, if_exists="replace", index=False)
            conn.execute(
                "INSERT OR IGNORE INTO index_daily (date, code, close, pctChg) SELECT date, code, close, pctChg FROM temp_index")
    bs.logout()

    # --- 步骤 2: 同步 ETF 行情 (AkShare) ---
    print(f"\n[2/2] 开始同步 ETF 行情（共 {len(etf_codes)} 只）")
    ak_end_date = END_DATE.replace("-", "")

    for i, full_code in enumerate(tqdm(etf_codes, desc="同步进度", unit="只")):
        start_date_raw = last_update_map.get(full_code, DEFAULT_START)

        # 闪电跳过
        if start_date_raw != DEFAULT_START and latest_market_date != "1990-01-01":
            if start_date_raw >= latest_market_date:
                continue

        try:
            symbol = full_code.split('.')[1]
            ak_start_date = start_date_raw.replace("-", "")

            # AkShare 抓取数据
            df = ak.fund_etf_hist_em(symbol=symbol, period="daily",
                                     start_date=ak_start_date, end_date=ak_end_date, adjust="qfq")

            if not df.empty:
                # 【关键修复1】: 确保日期格式严格为 YYYY-MM-DD
                df['日期'] = pd.to_datetime(df['日期']).dt.strftime('%Y-%m-%d')

                # 【关键修复2】: AkShare 没有"昨收"列，通过 "收盘 - 涨跌额" 自动计算
                if '昨收' not in df.columns:
                    if '涨跌额' in df.columns:
                        df['昨收'] = df['收盘'] - df['涨跌额']
                    else:
                        df['昨收'] = np.nan  # 极端情况兜底

                # 安全提取需要的列 (.copy() 防止警告)
                expected_cols = ['日期', '开盘', '最高', '最低', '收盘', '昨收', '成交量', '成交额', '换手率', '涨跌幅']

                # 如果缺少换手率等非致命字段，补 NaN
                for col in expected_cols:
                    if col not in df.columns:
                        df[col] = np.nan

                df = df[expected_cols].copy()

                # 重命名为数据库字段
                df.columns = ['date', 'open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'turn', 'pctChg']
                df['code'] = full_code

                # 转换数值
                numeric_cols = ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'turn', 'pctChg']
                df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

                # 写入数据库
                df.to_sql("temp_daily", conn, if_exists="replace", index=False)

                # 明确指定 SELECT 顺序写入主表
                sql = """
                      INSERT \
                      OR IGNORE INTO etf_daily 
                (date, code, open, high, low, close, preclose, volume, amount, turn, pctChg)
                      SELECT date, code, open, high, low, close, preclose, volume, amount, turn, pctChg
                      FROM temp_daily \
                      """
                conn.execute(sql)

        except Exception as e:
            # print(f"错误 {full_code}: {e}") # 调试时可打开
            continue

        if (i + 1) % 100 == 0:
            conn.commit()

    conn.execute("DROP TABLE IF EXISTS temp_daily")
    conn.execute("DROP TABLE IF EXISTS temp_index")
    conn.commit()
    conn.close()
    print(f"\n✨ 同步完成！数据已更新至最新。")


if __name__ == "__main__":
    sync_data()