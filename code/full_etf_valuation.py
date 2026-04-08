import akshare as ak
import pandas as pd
import sqlite3
import os
import numpy as np
import time
from datetime import datetime, timedelta

# --- 1. 路径配置 ---
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
DB_PATH = os.path.join(root_dir, "data", "stock_data.db")


def format_stock_code(code):
    c = str(code).zfill(6)
    return f"sh.{c}" if c.startswith('6') else f"sz.{c}"


def get_market_factors(etf_code, conn):
    """从数据库计算该 ETF 的行情因子"""
    # 加载足够的数据计算 5 年大底和 60 日均量
    query = f"SELECT * FROM etf_daily WHERE code = '{etf_code}' ORDER BY date ASC"
    df = pd.read_sql(query, conn)
    if df.empty or len(df) < 60: return None

    # 数值转换
    cols = ['open', 'high', 'low', 'close', 'amount', 'pctChg']
    df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')

    # 计算因子
    # 1. 成交量相关
    df['avg_amt_5d'] = df['amount'].rolling(5).mean() / 1e8
    df['avg_amt_60d'] = df['amount'].rolling(60).mean() / 1e8
    vol_surge = df['avg_amt_5d'].iloc[-1] / df['avg_amt_60d'].iloc[-1]

    # 2. 收益与波动
    ret_20d = (df['close'].iloc[-1] / df['close'].iloc[-21] - 1) * 100
    vol_20d = df['pctChg'].tail(20).std() * np.sqrt(242)

    # 3. 20d胜率 (上涨天数占比)
    win_rate = (df['pctChg'].tail(20) > 0).sum() / 20 * 100

    # 4. 距底距离 (5年大底)
    #low_long_term = df['low'].rolling(1200, min_periods=60).min().iloc[-1]
    #dist_bottom = (df['close'].iloc[-1] - low_long_term) / low_long_term * 100

    return {
        "5日均额": df['avg_amt_5d'].iloc[-1],
        "放量倍数": vol_surge,
        "20d涨幅": ret_20d,
        "20d胜率": win_rate,
        "年化波动": vol_20d,
        #"距底位置": dist_bottom
    }


def get_valuation_factors(etf_code, conn):
    """计算该 ETF 的穿透估值因子"""
    try:
        symbol = etf_code.split(".")[1]
        df_holdings = ak.fund_portfolio_hold_em(symbol=symbol)
        if df_holdings.empty: return None

        df_holdings['db_code'] = df_holdings['股票代码'].apply(format_stock_code)
        weights = dict(zip(df_holdings['db_code'], df_holdings['占净值比例']))
        stock_codes = list(weights.keys())

        # 查股票 PE/PB (7年)
        placeholders = ','.join(f"'{c}'" for c in stock_codes)
        query = f"SELECT date, code, peTTM, pbMRQ FROM stock_daily WHERE code IN ({placeholders}) AND peTTM>0"
        df_hist = pd.read_sql(query, conn)
        if df_hist.empty: return None

        df_pe = df_hist.pivot(index='date', columns='code', values='peTTM')
        df_pb = df_hist.pivot(index='date', columns='code', values='pbMRQ')
        df_w = pd.DataFrame(weights, index=df_pe.index)

        def calc_harmonic(df_val):
            mask = df_val.notna()
            valid_w = df_w[mask].sum(axis=1)
            valid_days = valid_w > 50
            if not valid_days.any(): return None
            contrib = (df_w[valid_days] / df_val[valid_days]).sum(axis=1)
            return valid_w[valid_days] / contrib

        pe_ts = calc_harmonic(df_pe)
        pb_ts = calc_harmonic(df_pb)

        if pe_ts is None: return None

        cur_pe = pe_ts.iloc[-1]
        cur_pb = pb_ts.iloc[-1]
        rank_pe = (pe_ts < cur_pe).mean() * 100
        rank_pb = (pb_ts < cur_pb).mean() * 100

        return {"PE": cur_pe, "PE分位": rank_pe, "PB": cur_pb, "PB分位": rank_pb}
    except:
        return None


def deep_analyze_list(code_list):
    """对传入的代码列表进行深度全维度分析"""
    conn = sqlite3.connect(DB_PATH)
    final_results = []

    print(f"🚀 正在对指定的 {len(code_list)} 只 ETF 进行全维度深度体检...")

    for code in code_list:
        # 获取名称
        name_query = f"SELECT code_name FROM etf_info WHERE code='{code}'"
        res_name = pd.read_sql(name_query, conn)
        name = res_name.iloc[0, 0] if not res_name.empty else "未知"

        # 1. 计算行情因子
        mkt = get_market_factors(code, conn)
        # 2. 计算估值因子
        val = get_valuation_factors(code, conn)

        if mkt:
            row = {
                "代码": code,
                "名称": name,
                "5日均额": f"{mkt['5日均额']:.2f}亿",
                "放量": f"{mkt['放量倍数']:.1f}倍",
                "20d涨幅": f"{mkt['20d涨幅']:.1f}%",
                "20d胜率": f"{mkt['20d胜率']:.0f}%",
                "波动率": f"{mkt['年化波动']:.1f}%",
                #"距底": f"底上{mkt['距底位置']:.1f}%"
            }
            if val:
                row.update({
                    "PE": f"{val['PE']:.1f}",
                    "PE分位": f"{val['PE分位']:.1f}%",
                    "PB": f"{val['PB']:.1f}",
                    "PB分位": f"{val['PB分位']:.1f}%"
                })
            else:
                row.update({"PE": "N/A", "PE分位": "N/A", "PB": "N/A", "PB分位": "N/A"})

            final_results.append(row)

    conn.close()

    # 输出报表
    print("\n" + "📊 终极深度体检综合报表 📊".center(110))
    print("=" * 120)
    report_df = pd.DataFrame(final_results)
    if not report_df.empty:
        # 调整列顺序，让最重要的在前面
        cols = ['代码', '名称', '5日均额', '放量', '20d涨幅', '20d胜率', 'PE', 'PE分位', 'PB', 'PB分位',
                '波动率']
        print(report_df[cols].to_string(index=False))
    else:
        print("未找到有效数据。")
    print("=" * 120)


if __name__ == "__main__":
    # --- 你在这里填入你想分析的任何 ETF 代码 ---
    my_watchlist = [
        "sh.510360",  # 沪深300
        "sz.159952",  # 创业板
        "sz.159766",  # 旅游ETF
        "sh.515980",  # 人工智能ETF
    ]

    deep_analyze_list(my_watchlist)