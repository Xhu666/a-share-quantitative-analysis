import akshare as ak
import os

# --- 1. 【核心：解决 ProxyError】强制关闭 Python 对系统代理的使用 ---
# 无论你是否开着 VPN，这两行代码都会让 Python 直接连接互联网
os.environ['HTTP_PROXY'] = ""
os.environ['HTTPS_PROXY'] = ""
os.environ['no_proxy'] = "*"


def test_fetch():
    try:
        print("🚀 正在尝试连接东方财富...")
        # --- 2. 【核心：修正参数】symbol 传 159852，不要传 sz.159852 ---
        df = ak.fund_etf_spot_em()


        if df.empty:
            print("⚠️ 抓取成功，但数据为空。请确认 2-24 是否是交易日。")
        else:
            print("✅ 抓取成功！数据如下：")
            print(df)

    except Exception as e:
        print(f"❌ 依然报错: {e}")


if __name__ == "__main__":
    test_fetch()