import baostock as bs
import pandas as pd
import sqlite3
import os
import time
from datetime import datetime, timedelta
from tqdm import tqdm

# --- 1. 路径配置 ---
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
db_dir = os.path.join(root_dir, "data")
DB_PATH = os.path.join(db_dir, "stock_data.db")

# --- 2. 目标回填起始点 (建议 7 年，覆盖上一轮牛熊) ---
# 假设我们要补到 2019-01-01
TARGET_START_DATE = (datetime.now() - timedelta(days=7 * 365)).strftime("%Y-%m-%d")


def get_current_min_dates(conn):
    """查询数据库中，每只股票目前最早的日期是哪一天"""
    print("📊 正在扫描数据库，计算每只股票的历史缺口...")
    try:
        # 获取每只票的 MIN(date)
        df = pd.read_sql("SELECT code, MIN(date) as first_date FROM stock_daily GROUP BY code", conn)
        return dict(zip(df['code'], df['first_date']))
    except Exception as e:
        print(f"读取数据库失败: {e}")
        return {}


def backfill_stock_history():
    conn = sqlite3.connect(DB_PATH)

    # 1. 获取所有股票代码
    try:
        stock_list = pd.read_sql("SELECT code FROM stock_info", conn)
        stock_codes = stock_list['code'].tolist()
    except:
        print("❌ 无法读取股票列表，请先运行 sync_stock.py 至少一次以获取名单。")
        return

    # 2. 获取现状：每只票最早存到了哪天
    min_date_map = get_current_min_dates(conn)

    print(f"🎯 目标回填起始日期: {TARGET_START_DATE}")
    print(f"🚀 开始对 {len(stock_codes)} 只股票进行历史回填...")

    bs.login()

    # 进度条
    for i, code in enumerate(tqdm(stock_codes, desc="回填进度", unit="只")):
        # 获取该股票目前最早的日期
        # 如果数据库里没有这只票，默认它最早日期是“今天”，所以要从头补到今天
        current_first_date = min_date_map.get(code, datetime.now().strftime("%Y-%m-%d"))

        # 判断是否需要回填
        # 如果目前最早的日期(比如2023-01-01) 晚于 目标日期(2019-01-01)，说明有缺口
        if current_first_date > TARGET_START_DATE:
            try:
                # 下载区间：[目标七年前] -> [目前最早日期]
                # 注意：BaoStock 是闭区间，可能会重复下载 current_first_date 这一天
                # 但我们有 INSERT OR IGNORE，重复的一天会被自动丢弃，完美缝合
                rs = bs.query_history_k_data_plus(
                    code,
                    "date,code,open,high,low,close,preclose,volume,amount,turn,pctChg,peTTM,pbMRQ",
                    start_date=TARGET_START_DATE,
                    end_date=current_first_date,
                    frequency="d", adjustflag="2"
                )

                data_list = []
                while (rs.error_code == '0') & rs.next():
                    data_list.append(rs.get_row_data())

                if data_list:
                    df = pd.DataFrame(data_list, columns=rs.fields)

                    # 数值转换
                    numeric_cols = ['open', 'high', 'low', 'close', 'preclose', 'volume', 'amount', 'turn', 'pctChg',
                                    'peTTM', 'pbMRQ']
                    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

                    # 写入临时表
                    df.to_sql("temp_backfill", conn, if_exists="replace", index=False)

                    # 核心：INSERT OR IGNORE 负责去重缝合
                    sql = """
                          INSERT \
                          OR IGNORE INTO stock_daily 
                    (date, code, open, high, low, close, preclose, volume, amount, turn, pctChg, peTTM, pbMRQ)
                          SELECT date, code, open, high, low, close, preclose, volume, amount, turn, pctChg, peTTM, pbMRQ
                          FROM temp_backfill \
                          """
                    conn.execute(sql)

            except Exception as e:
                # print(f"错误 {code}: {e}")
                continue

        # 每50只提交一次
        if (i + 1) % 50 == 0:
            conn.commit()

    conn.execute("DROP TABLE IF EXISTS temp_backfill")
    conn.commit()
    conn.close()
    bs.logout()
    print(f"\n✨ 历史回填完成！现在你的数据库覆盖范围已达: {TARGET_START_DATE} 至今。")


if __name__ == "__main__":
    backfill_stock_history()