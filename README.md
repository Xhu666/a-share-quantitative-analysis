# A-Share Quantitative Analysis

A quantitative stock screening and ETF analysis system for China A-share market. Built with Python, powered by [BaoStock](http://baostock.com/) and [AkShare](https://github.com/akfamily/akshare) data sources, with local SQLite storage.

## Features

- **Data Synchronization** -- Incremental sync of 5,400+ A-share stocks and 1,400+ ETFs with daily OHLCV data from BaoStock and AkShare
- **ETF Smart Selector** -- Two-stage funnel screening based on momentum, volume, win-rate, and cross-holding valuation (weighted PE/PB percentiles)
- **Correlation Analysis** -- ETF return correlation matrix with heatmap visualization
- **Tech Stock Screener** -- Aggressive stock screening with bullish MA alignment, volume breakout, and low-price filters
- **Defense Stock Screener** -- RPS (Relative Price Strength) based screening to identify potential leading stocks at breakout points
- **Deep ETF Valuation** -- Look-through valuation by weighting constituent stock PE/PB from ETF holdings

## System Architecture

```
Data Layer          Factor Layer              Strategy Layer
+------------+     +--------------------+    +-----------------------+
| sync_stock |     |                    |    |                       |
| back_fill  | --> |                    | -> | tech_stock_selector   |
+------------+     | full_etf_valuation |    | (aggressive screening)|
+------------+     |                    |    +-----------------------+
| sync_etf   | --> |                    | -> | defense_stock_selector|
+------------+     +--------------------+    | (RPS leader hunting)  |
                   | etf_correlation    |    +-----------------------+
                   | (heatmap)          | -> | etf_selector          |
                   +--------------------+    | (smart ETF picking)   |
                                             +-----------------------+
                      All reading/writing via SQLite (stock_data.db)
```

## Directory Structure

```
.
├── code/
│   ├── sync_stock.py              # Sync A-share daily data from BaoStock
│   ├── back_fill_stock.py         # Backfill stock history to 7 years
│   ├── sync_etf.py                # Sync ETF & index data from BaoStock/AkShare
│   ├── etf_selector.py            # Two-stage ETF screening pipeline
│   ├── full_etf_valuation.py      # Deep ETF valuation (look-through PE/PB)
│   ├── etf_correlation_tool.py    # ETF correlation matrix & heatmap
│   ├── tech_stock_selector.py     # Aggressive stock screener (MA alignment)
│   └── defense_stock_selector.py  # Defensive stock screener (RPS ranking)
├── data/
│   └── stock_data.db              # SQLite database (~1.35 GB)
├── requirements.txt
└── .gitignore
```

## Quick Start

### Prerequisites

- Python 3.8+
- Internet connection (for data fetching)

### Installation

```bash
git clone https://github.com/Xhu666/a-share-quantitative-analysis.git
cd a-share-quantitative-analysis
pip install -r requirements.txt
```

### Usage

**Step 1: Sync market data**

```bash
cd code

# Sync A-share stock data (5 years by default)
python sync_stock.py

# Optional: backfill to 7 years for a full bull-bear cycle
python back_fill_stock.py

# Sync ETF and index data
python sync_etf.py
```

**Step 2: Run screening strategies**

```bash
# ETF smart selection with deep valuation
python etf_selector.py

# Aggressive stock screening (bullish breakout)
python tech_stock_selector.py

# Defensive stock screening (RPS-based leader hunting)
python defense_stock_selector.py

# ETF correlation analysis (generates heatmap)
python etf_correlation_tool.py

# Deep valuation for specific ETFs
python full_etf_valuation.py
```

## Strategy Details

### ETF Smart Selector (`etf_selector.py`)

A two-stage funnel approach:
1. **Stage 1 -- Momentum Screening**: 5-day avg turnover > 50M CNY, volume ratio > 1.2x, 20-day win rate >= 55%, uptrend, exclude bond/money-market ETFs
2. **Correlation Deduplication**: Remove highly correlated ETFs (> 0.90 correlation), keeping the one with higher turnover
3. **Stage 2 -- Deep Valuation**: Look-through PE/PB calculation using ETF holdings with historical percentile ranking

### Tech Stock Screener (`tech_stock_selector.py`)

Aggressive screening for breakout stocks:
- Price < 20 CNY (low-price bias)
- MA5 >= MA10 >= MA20 >= MA30 (bullish alignment)
- Volume ratio > 1.3x (volume breakout confirmation)
- 3-day avg turnover > 80M CNY (sufficient liquidity)
- Exclude ST/delisting stocks

### Defense Stock Screener (`defense_stock_selector.py`)

RPS-based potential leader identification:
- RPS120 > 85 (top 15% relative strength over 120 days)
- Volume ratio > 1.4x (institutional interest signal)
- Price within 10% of MA20 (not overextended)
- Price above MA60 (medium-term uptrend intact)
- Daily turnover > 60M CNY

## Data Sources

| Source | Data | Coverage |
|--------|------|----------|
| [BaoStock](http://baostock.com/) | A-share daily OHLCV + PE/PB, financial reports, index data | Full market |
| [AkShare](https://github.com/akfamily/akshare) | ETF daily OHLCV, ETF holdings/weighting | All exchange-traded ETFs |

Data is stored locally in SQLite with incremental sync -- only new records are fetched on each run.

## Disclaimer

This project is for **educational and research purposes only**. It does not constitute investment advice, financial advice, trading advice, or any other sort of advice.

- Past performance does not guarantee future results
- The screening results are based on historical data and technical factors, which may not reflect future market conditions
- The author is not responsible for any investment decisions made based on this tool
- Always do your own research and consult with a qualified financial advisor before making investment decisions

Use at your own risk.
