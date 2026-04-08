import sqlite3
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os

# --- 1. 路径配置 ---
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
DB_PATH = os.path.join(root_dir, "data", "stock_data.db")

# 设置中文字体（防止绘图乱码）
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False


def get_etf_correlation():
    # --- 2. 输入环节 ---
    raw_code = ['510360', #沪深300
                '159766', #旅游ETF
                '515980', #人工智能ETF
                '159326', #电网设备ETF
                '159941',  # 纳指ETF
                '159699',  # 恒生消费ETF
                '515120',   #创新药ETF
                '513120'    #港股创新药ETF
                ]

    # 使用列表推导式生成新的格式化代码列表，安全且高效
    code = [
        f"sh.{c}" if c.startswith(('5', '6')) else f"sz.{c}"
        if not c.startswith(('sh.', 'sz.')) else c
        for c in raw_code
    ]

    print(f"✅ 正在分析以下代码: {code}")

    # --- 3. 数据库查询 ---
    conn = sqlite3.connect(DB_PATH)
    placeholders = ','.join(f"'{c}'" for c in code)

    # 获取近一年数据，并关联名称
    query = f"""
    SELECT a.date, b.code_name as name, a.close
    FROM etf_daily a
    JOIN etf_info b ON a.code = b.code
    WHERE a.code IN ({placeholders}) 
      AND a.date >= date('now', '-1825 day')
    ORDER BY a.date ASC
    """
    df = pd.read_sql(query, conn)
    conn.close()

    if df.empty:
        print("❌ 数据库中未找到这些代码的数据，请确认已运行过同步脚本。")
        return

    # --- 4. 数据透视 ---
    # 将数据转为：日期为索引，名称为列，价格为值
    pivot_df = df.pivot(index='date', columns='name', values='close')

    # 核心：计算收益率的相关性（量化中必须用收益率算相关性，不能直接用价格）
    corr_matrix = pivot_df.pct_change().corr()

    # --- 5. 绘图 ---
    plt.figure(figsize=(10, 8))
    sns.heatmap(corr_matrix,
                annot=True,  # 显示数字
                fmt=".2f",  # 保留两位小数
                cmap='RdBu_r',  # 红蓝配色（红正相关，蓝负相关）
                center=0,  # 0为颜色中点
                linewidths=0.5)  # 格子间距

    plt.title(f"ETF 收益率相关性矩阵 (近1年数据)", fontsize=15)
    plt.xticks(rotation=45)
    plt.tight_layout()

    print("\n✅ 相关性矩阵计算完成：")
    print(corr_matrix)
    print("\n💡 正在生成热力图，请查看弹窗...")
    plt.show()


if __name__ == "__main__":
    get_etf_correlation()