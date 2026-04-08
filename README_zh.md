# A股量化分析系统

[English](README.md)

基于 Python 的A股量化选股与ETF分析系统。数据来源 [BaoStock](http://baostock.com/) 和 [AkShare](https://github.com/akfamily/akshare)，本地 SQLite 存储。

## 功能特性

- **数据同步** -- 增量同步 5400+ 只A股个股和 1400+ 只 ETF 的日线行情数据
- **ETF智能选基** -- 两阶段漏斗筛选，基于动量、放量、胜率、穿透估值（持仓加权PE/PB分位）
- **相关性分析** -- ETF 日收益率相关性矩阵，热力图可视化
- **进攻型选股** -- 均线多头排列 + 放量突破 + 低价股筛选
- **防御型选股** -- 基于RPS相对强度排名，寻找潜在龙头起爆点
- **ETF深度估值** -- 穿透持仓计算加权PE/PB及其历史分位

## 系统架构

```
数据层              因子层                    策略层
+------------+     +--------------------+    +-----------------------+
| sync_stock |     |                    |    |                       |
| back_fill  | --> |                    | -> | tech_stock_selector   |
+------------+     | full_etf_valuation |    | (进攻型选股)          |
+------------+     |                    |    +-----------------------+
| sync_etf   | --> |                    | -> | defense_stock_selector|
+------------+     +--------------------+    | (防御型选股/RPS龙头)  |
                   | etf_correlation    |    +-----------------------+
                   | (相关性热力图)     | -> | etf_selector          |
                   +--------------------+    | (ETF智能选基)         |
                                             +-----------------------+
                      所有数据读写通过 SQLite (stock_data.db)
```

## 目录结构

```
.
├── code/
│   ├── sync_stock.py              # A股个股数据同步（BaoStock -> SQLite）
│   ├── back_fill_stock.py         # 个股历史数据回填至7年
│   ├── sync_etf.py                # ETF + 指数数据同步
│   ├── etf_selector.py            # 两阶段ETF智能选基
│   ├── full_etf_valuation.py      # ETF全维度深度估值体检
│   ├── etf_correlation_tool.py    # ETF收益率相关性分析（热力图）
│   ├── tech_stock_selector.py     # 进攻型个股选股器（均线多头）
│   └── defense_stock_selector.py  # 防御型个股选股器（RPS龙头）
├── data/
│   └── stock_data.db              # SQLite 数据库（约1.35GB）
├── requirements.txt
└── .gitignore
```

## 快速开始

### 环境要求

- Python 3.8+
- 网络连接（用于拉取行情数据）

### 安装

```bash
git clone https://github.com/Xhu666/a-share-quantitative-analysis.git
cd a-share-quantitative-analysis
pip install -r requirements.txt
```

### 使用方法

**第一步：同步市场数据**

```bash
cd code

# 同步A股个股数据（默认近5年）
python sync_stock.py

# 可选：回填至7年，覆盖完整牛熊周期
python back_fill_stock.py

# 同步ETF和指数数据
python sync_etf.py
```

**第二步：运行选股策略**

```bash
# ETF智能选基（含深度估值）
python etf_selector.py

# 进攻型个股筛选（放量突破）
python tech_stock_selector.py

# 防御型个股筛选（RPS龙头捕捉）
python defense_stock_selector.py

# ETF相关性分析（生成热力图）
python etf_correlation_tool.py

# ETF深度估值分析
python full_etf_valuation.py
```

## 策略详解

### ETF智能选基 (`etf_selector.py`)

两阶段漏斗筛选：
1. **第一阶段 -- 动量初筛**：5日均成交额 > 5000万、放量倍数 > 1.2、20日胜率 >= 55%、趋势向上、排除债券/货币/理财型ETF
2. **相关性去重**：在候选ETF中计算日收益率相关矩阵，若两只ETF相关性 > 0.90，保留成交额更大的那只
3. **第二阶段 -- 深度估值**：穿透ETF持仓计算加权PE/PB，并得出历史分位排名

### 进攻型选股 (`tech_stock_selector.py`)

适合捕捉放量突破的进攻姿态个股：
- 收盘价 < 20元（低价股偏好）
- MA5 >= MA10 >= MA20 >= MA30（均线多头排列）
- 放量倍数 > 1.3（放量确认）
- 3日均成交额 > 8000万（流动性充足）
- 排除ST/退市股

### 防御型选股 (`defense_stock_selector.py`)

基于RPS排名寻找潜在龙头起爆点：
- RPS120 > 85（120日涨幅排名前15%）
- 放量倍数 > 1.4（机构关注信号）
- 距MA20不超过10%（防止追高）
- 价格在MA60之上（中期趋势完好）
- 日成交额 > 6000万

## 数据来源

| 数据源 | 提供数据 | 覆盖范围 |
|--------|---------|---------|
| [BaoStock](http://baostock.com/) | A股日线OHLCV + PE/PB、财报数据、指数数据 | 全市场 |
| [AkShare](https://github.com/akfamily/akshare) | ETF日线OHLCV、ETF持仓及权重 | 所有场内ETF |

数据以增量同步方式存储在本地 SQLite 中，每次运行仅拉取缺失的新数据。

## 免责声明

本项目仅供**学习和研究用途**，不构成任何投资建议、理财建议或交易建议。

- 历史表现不代表未来收益
- 筛选结果基于历史数据和技术因子，可能无法反映未来市场情况
- 作者不对基于本工具做出的任何投资决策负责
- 投资有风险，入市需谨慎，请务必自行研究并咨询专业理财顾问

风险自负。
