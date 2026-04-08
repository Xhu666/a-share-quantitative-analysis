import pandas as pd
import sqlite3
import os
import baostock as bs
import numpy as np
from tqdm import tqdm

# --- 1. 路径配置 ---
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
DB_PATH = os.path.join(root_dir, "data", "stock_data.db")


def load_all_data():
    conn = sqlite3.connect(DB_PATH)
    print("⏳ 正在读取 7 年个股历史数据（构建进攻坐标系）...")
    query = """
            SELECT a.date, \
                   a.code, \
                   a.close, \
                   a.amount, \
                   a.pctChg, \
                   a.peTTM, \
                   a.pbMRQ, \
                   b.code_name
            FROM stock_daily a
                     JOIN stock_info b ON a.code = b.code
            WHERE a.date >= date ('now', '-2 months')
            """
    df = pd.read_sql(query, conn)
    conn.close()
    numeric_cols = ['close', 'amount', 'pctChg', 'peTTM', 'pbMRQ']
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    return df


def calculate_attack_factors(df):
    print("🧪 正在计算弹性因子与动能指标...")
    df = df.sort_values(['code', 'date']).reset_index(drop=True)

    # 1. 估值位置（进攻股我们放宽到 60% 分位，给成长空间）
    #df['pe_rank'] = df.groupby('code')['peTTM'].rank(pct=True) * 100

    # 2. 进攻动能：短期均线必须多头向上
    df['ma5'] = df.groupby('code')['close'].transform(lambda x: x.rolling(5).mean())
    df['ma10'] = df.groupby('code')['close'].transform(lambda x: x.rolling(10).mean())
    df['ma20'] = df.groupby('code')['close'].transform(lambda x: x.rolling(20).mean())
    df['ma30'] = df.groupby('code')['close'].transform(lambda x: x.rolling(30).mean())

    # 3. 弹性因子：近20日年化波动率（越高代表弹性越大）
    #df['volatility'] = df.groupby('code')['pctChg'].transform(lambda x: x.rolling(20).std()) * np.sqrt(242)

    # 4. 能量爆发：近期放量情况
    df['avg_amt_3d'] = df.groupby('code')['amount'].transform(lambda x: x.rolling(3).mean())
    df['avg_amt_10d'] = df.groupby('code')['amount'].transform(lambda x: x.rolling(10).mean())
    df['vol_surge'] = df['avg_amt_3d'] / df['avg_amt_10d']

    latest = df.groupby('code').last().reset_index()
    return latest


def do_attack_screening(df):
    """初筛：寻找处于‘进攻姿态’的个股"""
    mask = (
        # --- 1. 估值必须合理（拒绝纯情绪炒作） ---
            #(df['pe_rank'] < 45) &  # PE 分位不超过 65%，确保没涨到天上去
            #(df['peTTM'] > 5) & (df['peTTM'] < 50) &  # 拒绝僵尸股，也拒绝估值过高的梦境股

            # --- 2. 强力进攻形态（多头排列） ---
            #(df['close'] > df['ma20']) &  # 价格在 20 日线上
            (df['close'] < 20.0) &  # 价格
            (df['ma5'] >= df['ma10']) &
            (df['ma10'] >= df['ma20']) &
            (df['ma20'] >= df['ma30']) &

            # --- 3. 资金已经点火 ---
            (df['vol_surge'] > 1.3) &  # 5日均量比60日放大了20%以上

            # --- 4. 基础过滤 ---
            (~df['code_name'].str.contains('ST|退')) &
            (df['avg_amt_3d'] > 80000000)  # 进攻股需要更好的流动性，门槛提至 8000 万
    )
    results = df[mask].copy()
    return results


def get_attack_financials(code, year, quarter, depth=4):
    """
    抓取爆发力指标：高ROE (赚钱效率) + 高净利润增速
    """
    current_year, current_quarter = year, quarter
    for _ in range(depth):
        res_p = bs.query_profit_data(code=code, year=current_year, quarter=current_quarter)
        df_p = res_p.get_data()
        if not df_p.empty and df_p.iloc[0]['roeAvg'] != "":
            # 拿到 ROE 和 净利润增速
            res_g = bs.query_growth_data(code=code, year=current_year, quarter=current_quarter)
            df_g = res_g.get_data()
            if not df_g.empty:
                df_p['YOYNI'] = df_g.iloc[0].get('YOYNI', '0')  # 净利润同比增速
                df_p['YOYOR'] = df_g.iloc[0].get('YOYOR', '0')  # 营业收入同比增速
            return df_p
        if current_quarter > 1:
            current_quarter -= 1
        else:
            current_quarter = 4; current_year -= 1
    return pd.DataFrame()


def second_stage_attack_screening(df_first):
    bs.login()
    valid_results = []
    # 模拟目前是2026年，查询2025年报/三季报
    target_y, target_q = 2025, 4

    print(f"🔥 正在扫描 {len(df_first)} 只个股的业绩爆发力...")
    for _, row in tqdm(df_first.iterrows(), total=df_first.shape[0]):
        df_fin = get_attack_financials(row['code'], target_y, target_q)
        if df_fin.empty: continue
        try:
            roe = float(df_fin.iloc[0]['roeAvg'])
            profit_growth = float(df_fin.iloc[0]['YOYNI'])
            rev_growth = float(df_fin.iloc[0]['YOYOR'])

            # --- 进攻型核心指标判定 ---
            # 1. ROE > 15% (极其出色的赚钱效率)
            # 2. 净利润增速 > 30% (进入高速成长期)
            # 3. 营收增速 > 15% (确保不是靠卖房卖资产带来的利润假象)
            if roe > 0 and profit_growth > 0:
                # 计算 PEG (市盈率 / 净利润增速)
                # 进攻股最完美的形态是 PEG < 1
                peg = row['peTTM'] / (profit_growth * 100)

                if peg < 15:  # 稍微放宽到 1.2，寻找估值合理的高增长
                    row_dict = row.to_dict()
                    row_dict['roe'] = roe
                    row_dict['growth'] = profit_growth
                    row_dict['peg'] = peg
                    valid_results.append(row_dict)
        except:
            pass
    bs.logout()
    return pd.DataFrame(valid_results)


if __name__ == "__main__":
    raw_df = load_all_data()
    if not raw_df.empty:
        factors_df = calculate_attack_factors(raw_df)
        first_list = do_attack_screening(factors_df)
        print(f"\n✅ 选出 {len(first_list)} 只‘姿态正确’的进攻预备股。")

        #final_list = second_stage_attack_screening(first_list)
        final_list = first_list

        if not final_list.empty:
            cols = ['code', 'code_name', 'close', 'vol_surge']
            output = final_list[cols].copy()
            output.columns = ['代码', '名称', '昨收', '放量倍数']

            # 格式化
            '''output['ROE'] = output['ROE'].apply(lambda x: f"{x * 100:.1f}%")
            output['利润增速'] = output['利润增速'].apply(lambda x: f"{x * 100:.1f}%")
            output['波动率(弹性)'] = output['波动率(弹性)'].apply(lambda x: f"{x:.1f}%")'''

            # 按 PEG 升序排序：PEG 越低，性价比越高
            output = output.sort_values(by='昨收', ascending=True)

            print(f"\n🚀 【全场进攻核心】名单：共锁定 {len(final_list)} 只高增长、低PEG、高弹性股！\n")
            print(output.to_string(index=False))
            print("\n💡 建议：重点关注 PEG < 1 的标的，它们是市场中‘增长比估值跑得快’的明珠。")
        else:
            print("\n💡 未能找到符合‘高增长+低PEG’的品种。可能市场目前估值过热，或业绩期尚未披露。")