import pandas as pd
import sqlite3
import numpy as np
import os
from datetime import datetime, timedelta

# --- 1. 路径配置 ---
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
DB_PATH = os.path.join(root_dir, "data", "stock_data.db")


def find_potential_leaders():
    conn = sqlite3.connect(DB_PATH)

    # 1. 加载最近半年的数据（计算 RPS 需要历史纵深）
    print("⏳ 正在读取数据并构建相对强度坐标系...")
    query = """
            SELECT a.date, a.code, a.close, a.amount, a.pctChg, b.code_name
            FROM stock_daily a
                     JOIN stock_info b ON a.code = b.code
            WHERE a.date >= date ('now', '-200 day') \
            """
    df = pd.read_sql(query, conn)
    conn.close()

    if df.empty:
        print("❌ 数据库是空的，请先运行同步脚本！")
        return pd.DataFrame()

    # 数据预处理
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    df = df.sort_values(['code', 'date'])

    print("🧪 正在计算潜在龙头因子 (RPS + 能量异动)...")

    # --- 2. 计算行情指标 ---
    # 计算 120 日收益率 (用于算 RPS)
    # 我们用当前价格对比 120 个交易日前
    df['ret_120d'] = df.groupby('code')['close'].transform(lambda x: x.pct_change(120))

    # 计算均线
    df['ma20'] = df.groupby('code')['close'].transform(lambda x: x.rolling(20).mean())
    df['ma60'] = df.groupby('code')['close'].transform(lambda x: x.rolling(60).mean())

    # 计算成交额异动
    df['avg_amt_3d'] = df.groupby('code')['amount'].transform(lambda x: x.rolling(3).mean())
    df['avg_amt_6d'] = df.groupby('code')['amount'].transform(lambda x: x.rolling(6).mean())
    df['vol_surge'] = df['avg_amt_3d'] / df['avg_amt_6d']

    # --- 3. 提取最新数据行 ---
    latest = df.groupby('code').last().reset_index()

    # --- 4. 计算全市场相对强度排名 (RPS) ---
    # 去掉涨幅为 NaN 的新股
    valid_returns = latest.dropna(subset=['ret_120d'])
    latest['rps_120'] = latest['ret_120d'].rank(pct=True) * 100

    # 计算距离均线的距离（防追高）
    latest['dist_ma20'] = (latest['close'] / latest['ma20'] - 1) * 100

    # --- 5. 潜在龙头筛选漏斗 ---
    mask = (
            (latest['rps_120'] > 85) &  # 相对强度在前 15% (说明已经是热点板块)
            (latest['vol_surge'] > 1.4) &  # 近期有明显的大资金点火
            (latest['dist_ma20'] < 10) &  # 刚启动，离 20 日线还不远 (拒绝追高)
            (latest['close'] > latest['ma60']) &  # 站上牛熊线 (大趋势走好)
            (latest['amount'] > 60000000) &  # 日成交额 > 6000 万 (拒绝僵尸股)
            (~latest['code_name'].str.contains('ST|退'))
    )

    potential_leaders = latest[mask].copy()

    # 按照“放量”和“强度”综合排序
    return potential_leaders.sort_values(by=['vol_surge', 'rps_120'], ascending=False)


if __name__ == "__main__":
    results = find_potential_leaders()

    if not results.empty:
        print(f"\n✅ 选股完成！从 5000+ 只中为你锁定 {len(results)} 只‘起爆点’潜在龙头：\n")

        # 挑选重点列展示
        show_cols = ['code', 'code_name', 'rps_120', 'vol_surge', 'dist_ma20', 'amount']
        display_df = results[show_cols].head(20).copy()

        # 美化格式
        display_df['rps_120'] = display_df['rps_120'].apply(lambda x: f"{x:.1f}")
        display_df['vol_surge'] = display_df['vol_surge'].apply(lambda x: f"{x:.1f}倍")
        display_df['dist_ma20'] = display_df['dist_ma20'].apply(lambda x: f"{x:.1f}%")
        display_df['amount'] = display_df['amount'].apply(lambda x: f"{x / 100000000:.2f}亿")

        display_df.columns = ['代码', '名称', 'RPS120', '成交放量', '距20日线', '今日成交']

        print(display_df.to_string(index=False))
        print("\n💡 实战建议：重点看【成交放量】在 2 倍以上且【距20日线】在 5% 以内的品种。")
    else:
        print("💡 暂无匹配标的，市场当前可能处于调整期，建议耐心等待‘点火信号’。")