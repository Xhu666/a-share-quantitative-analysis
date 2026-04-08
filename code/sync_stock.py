import baostock as bs
import pandas as pd
import sqlite3
import os
import time
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


def init_stock_tables():
    """初始化个股专用数据库表结构"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # 个股基础信息表
    cursor.execute('CREATE TABLE IF NOT EXISTS stock_info (code TEXT PRIMARY KEY, code_name TEXT)')
    # 个股日线表 (增加了 peTTM 和 pbMRQ 估值字段)
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS stock_daily
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
                       peTTM
                       REAL,
                       pbMRQ
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


def get_a_share_list():
    """获取全市场 A 股个股名单"""
    bs.login()
    all_stock = pd.DataFrame()
    print("🔍 正在同步最新 A 股名单...")
    for i in range(0, 35):
        target_day = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
        rs = bs.query_all_stock(day=target_day)
        all_stock = rs.get_data()
        if not all_stock.empty and 'code' in all_stock.columns:
            print(f"✅ 成功获取 {target_day} 的全市场列表。")
            break
    bs.logout()

    if all_stock.empty: return pd.DataFrame()

    # 严格筛选 A 股个股：
    # sh.6 开头 (上证主板+科创板)
    # sz.0 开头 (深证主板)
    # sz.3 开头 (创业板)
    # bj 开头 (北交所)
    stock_df = all_stock[all_stock['code'].str.match(r'^sh\.6|^sz\.0|^sz\.3|^bj\.')].copy()
    return stock_df


def sync_stock_data():
    init_stock_tables()
    stock_list_df = get_a_share_list()
    if stock_list_df.empty:
        print("❌ 无法获取股票名单，同步终止。")
        return

    conn = sqlite3.connect(DB_PATH)
    stock_list_df[['code', 'code_name']].to_sql("stock_info", conn, if_exists="replace", index=False)
    stock_codes = stock_list_df['code'].tolist()

    # 获取数据库里已有的最后更新日期
    last_update_map = {}
    try:
        df_old = pd.read_sql("SELECT code, MAX(date) as last_date FROM stock_daily GROUP BY code", conn)
        last_update_map = dict(zip(df_old['code'], df_old['last_date']))
    except:
        pass

    # 确定市场最新交易日 (用上证指数)
    bs.login()
    latest_market_date = "1990-01-01"
    for i in range(0, 35):
        check_day = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
        rs_check = bs.query_history_k_data_plus("sh.000001", "date", start_date=check_day, end_date=check_day,
                                                frequency="d")
        res_df = rs_check.get_data()
        if not res_df.empty and 'date' in res_df.columns:
            latest_market_date = res_df['date'].max()
            break

    print(f"\n🚀 开始增量同步个股行情（共 {len(stock_codes)} 只，包含动态市盈率 PE 数据）")
    start_time = time.time()

    for i, full_code in enumerate(tqdm(stock_codes, desc="A股同步进度", unit="只")):
        start_date_raw = last_update_map.get(full_code, DEFAULT_START)

        # 闪电跳过
        if start_date_raw != DEFAULT_START and latest_market_date != "1990-01-01":
            if start_date_raw >= latest_market_date:
                continue

        try:
            # 抓取个股数据 (带 peTTM, pbMRQ 字段)
            rs = bs.query_history_k_data_plus(
                full_code,
                "date,code,open,high,low,close,preclose,volume,amount,turn,pctChg,peTTM,pbMRQ",
                start_date=start_date_raw, end_date=END_DATE,
                frequency="d", adjustflag="2"  # 2为前复权
            )

            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())

            if data_list:
                df = pd.DataFrame(data_list, columns=rs.fields)

                # 数值转换
                numeric_cols = ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'turn', 'pctChg',
                                'peTTM', 'pbMRQ']
                # 空字符串转为 NaN
                df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

                # 写入临时表然后插入主表
                df.to_sql("temp_stock", conn, if_exists="replace", index=False)

                sql = """
                      INSERT \
                      OR IGNORE INTO stock_daily 
                (date, code, open, high, low, close, preclose, volume, amount, turn, pctChg, peTTM, pbMRQ)
                      SELECT date, code, open, high, low, close, preclose, volume, amount, turn, pctChg, peTTM, pbMRQ
                      FROM temp_stock \
                      """
                conn.execute(sql)

        except Exception as e:
            continue

        if (i + 1) % 50 == 0:
            conn.commit()

    conn.execute("DROP TABLE IF EXISTS temp_stock")
    conn.commit()
    conn.close()
    bs.logout()
    print(f"\n✨ A 股个股同步完成！总耗时: {(time.time() - start_time) / 60:.1f} 分钟")


if __name__ == "__main__":
    sync_stock_data()