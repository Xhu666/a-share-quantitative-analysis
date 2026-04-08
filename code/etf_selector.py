import pandas as pd
import sqlite3
import os
import numpy as np
import re
from datetime import datetime, timedelta
from tqdm import tqdm
from full_etf_valuation import deep_analyze_list

# --- 1. 路径配置 ---
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
DB_PATH = os.path.join(root_dir, "data", "stock_data.db")


def load_data(days=1800):
    conn = sqlite3.connect(DB_PATH)
    # 增加 amount 字段的读取，相关性去重需要根据成交额优胜劣汰
    query = f"""
    SELECT a.*, b.code_name 
    FROM etf_daily a 
    JOIN etf_info b ON a.code = b.code
    WHERE a.date >= date('now', '-{days} day')
    ORDER BY a.code, a.date ASC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    # 强制数值化
    numeric_cols = ['open', 'high', 'low', 'close', 'amount', 'pctChg']
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    return df


def calculate_factors(df):
    df = df.sort_values(['code', 'date']).reset_index(drop=True)
    # 基础均线
    df['ma20'] = df.groupby('code')['close'].transform(lambda x: x.rolling(20).mean())
    df['ma60'] = df.groupby('code')['close'].transform(lambda x: x.rolling(60).mean())
    # 动能
    df['ret_20d'] = df.groupby('code')['close'].transform(lambda x: x.pct_change(20)) * 100
    # 波动率
    df['vol_20d'] = df.groupby('code')['pctChg'].transform(lambda x: x.rolling(20).std()) * np.sqrt(242)
    # 长期底线
    #df['low_long_term'] = df.groupby('code')['low'].transform(lambda x: x.rolling(1000, min_periods=60).min())
    # 5日成交额 vs 60日成交额 (放量倍数)
    df['avg_amount_5d'] = df.groupby('code')['amount'].transform(lambda x: x.rolling(5).mean()) / 1e8
    df['avg_amount_60d'] = df.groupby('code')['amount'].transform(lambda x: x.rolling(60).mean()) / 1e8
    df['vol_surge'] = df['avg_amount_5d'] / df['avg_amount_60d']
    # 平滑度
    df['is_up'] = (df['pctChg'] > 0).astype(int)
    df['smoothness_20d'] = df.groupby('code')['is_up'].transform(lambda x: x.rolling(20).sum() / 20)

    latest = df.groupby('code').last().reset_index()
   #latest['dist_to_bottom'] = (latest['close'] - latest['low_long_term']) / latest['low_long_term'] * 100
    latest['is_trend_up'] = (latest['close'] > latest['ma60']) & (latest['ma20'] > latest['ma60'])
    return latest


def deduplicate_by_correlation(candidates_df, raw_data, threshold=0.90):
    """
    【核心逻辑】：相关性去重
    1. 在候选名单中，按成交额从大到小排列。
    2. 依次计算两两之间的价格相关性。
    3. 如果相关性高于阈值，则视为同类，剔除成交额小的。
    """
    if len(candidates_df) <= 1:
        return candidates_df

    print(f"📊 正在计算 {len(candidates_df)} 只候选品种的相关性矩阵...")

    # 提取最近 250 个交易日的价格序列进行相关性分析
    codes = candidates_df['code'].tolist()
    # 过滤原始数据，只保留最近250天
    recent_data = raw_data[raw_data['code'].isin(codes)].copy()
    max_date = recent_data['date'].max()
    start_analysis_date = (pd.to_datetime(max_date) - timedelta(days=365)).strftime('%Y-%m-%d')
    recent_data = recent_data[recent_data['date'] >= start_analysis_date]

    # 透视表：行是日期，列是代码
    pivot_df = recent_data.pivot(index='date', columns='code', values='close')
    # 计算日收益率的相关性矩阵
    corr_matrix = pivot_df.pct_change().corr()

    # 按照 5日成交额 降序排列，确保我们优先选择流动性最好的
    sorted_candidates = candidates_df.sort_values(by='avg_amount_5d', ascending=False)

    keep_list = []
    dropped_set = set()

    for code in sorted_candidates['code'].tolist():
        if code in dropped_set:
            continue

        # 将此代码加入保留名单
        keep_list.append(code)

        # 找出所有与此代码相关性极高的品种
        if code in corr_matrix.columns:
            similar_codes = corr_matrix.index[corr_matrix[code] > threshold].tolist()
            for s_code in similar_codes:
                if s_code != code:
                    dropped_set.add(s_code)

    return candidates_df[candidates_df['code'].isin(keep_list)]


def screen_etfs(df_factors):
    # 你的原筛选逻辑
    mask = (
            (df_factors['avg_amount_5d'] > 0.5) &  # 调低到0.5亿，覆盖更多潜力板块
            #(df_factors['dist_to_bottom'] < 40) &
            (df_factors['vol_surge'] > 1.2) &
            (df_factors['smoothness_20d'] >= 0.55) &
            (df_factors['is_trend_up'] == True) &
            (~df_factors['code_name'].str.contains('债|货币|理财|短融'))
    )
    return df_factors[mask].copy()


if __name__ == "__main__":
    print("🔍 [第一阶段] 正在扫描全市场行情并初步筛选...")
    raw_data = load_data(days=1800)

    if raw_data.empty:
        print("❌ 数据库无数据")
    else:
        # 1. 计算本地行情因子 (MA, 距底距离等)
        df_factors = calculate_factors(raw_data)

        # 2. 漏斗筛选 (初步筛出符合底部异动的品种)
        initial_candidates = screen_etfs(df_factors)

        # 3. 相关性去重 (剔除走势重复的板块)
        final_candidates_df = deduplicate_by_correlation(initial_candidates, raw_data, threshold=0.90)

        if final_candidates_df.empty:
            print("💡 当前市场没有符合底部启动条件的标的。")
        else:
            # 4. 【核心联动点】
            # 提取去重后的代码列表，例如前 15 只
            target_codes = final_candidates_df['code'].tolist()

            print(f"\n🎯 [第二阶段] 初步选出 {len(target_codes)} 只候选品种，开始执行全维度体检...")

            # 直接调用你的新版体检程序，它会自动输出那张大表
            deep_analyze_list(target_codes)
