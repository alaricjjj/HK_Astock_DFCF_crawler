__Author__ = "Alaric"

from datetime import datetime
import requests, json, re, time
import pandas as pd
import time
import random
import os

"""
解析网址
https://datacenter-web.eastmoney.com/api/data/v1/get?
callback=jQuery1123009597432045781096_1660960497724&
sortColumns=TRADE_DATE&
sortTypes=-1&
pageSize=10&
pageNumber=2&
reportName=RPT_MUTUAL_DEAL_HISTORY&
columns=ALL&
source=WEB&
client=WEB&
filter=(MUTUAL_TYPE%3D%22001%22)
"""

init_url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
params = {
    "callback": "jQuery1123009597432045781096_1660960497724",
    "sortColumns": "TRADE_DATE",
    "sortTypes": -1,
    "pageSize": 200,
    "pageNumber": 1,
    "reportName": "RPT_MUTUAL_DEAL_HISTORY",
    "columns": "ALL",
    "source": "WEB",
    "client": "WEB",
    # 'filter': '(MUTUAL_TYPE="001")', # 001:沪股通；002：港股通（沪）；003：深股通；004：港股通（深）
}

# 初始数据框
df_stock = pd.DataFrame()
while True:
    try:
        print(f"正在访问第{params['pageNumber']}页")
        # 获取url内容
        r = requests.get(url=init_url, params=params, timeout=30)
        # 内容解析
        content = re.sub(r"\s", "", r.content.decode())  # 删除任意空白字符， [\t\n\r\f]
        content = re.findall(r"\(({.*})\)", content)[0]  # 提取字典部分
        js_content = json.loads(content)
    except Exception as e:
        print(f"出现{e}报错，暂停访问，保存已获取数据")
        break

    if js_content["result"] is not None:
        # 数据整理
        data = js_content["result"]["data"]
        # 将数据整理成表格
        df = pd.DataFrame(data)
        df_stock = pd.concat([df_stock, df], ignore_index=True)
    else:
        print("===数据已获取完毕===")
        break
    params["pageNumber"] += 1
    time.sleep(random.randint(1, 5))

df_stock["TRADE_DATE"] = pd.to_datetime(
    df_stock["TRADE_DATE"], format="%Y-%m-%d%H:%M:%S"
)  # 设置为日期格式

# 设置多重索引
df_stock.set_index(["TRADE_DATE", "MUTUAL_TYPE"], inplace=True)
print(df_stock)

# 重命名
col_name = {"001": "hk2sh", "002": "sh2hk", "003": "hk2sz", "004": "sz2hk"}
df_stock = df_stock.rename(index=col_name, level=1)
df_stock.drop(index=["005", "006"], level=1, inplace=True)
print(df_stock)

col_name = {
    "FUND_INFLOW": "当日资金流入",
    "NET_DEAL_AMT": "当日成交净买额",
    "QUOTA_BALANCE": "当日余额",
    "ACCUM_DEAL_AMT": "历史累计净买额",
    "BUY_AMT": "买入成交额",
    "SELL_AMT": "卖出成交额",
    "LEAD_STOCKS_CODE": "领涨股代码",
    "LEAD_STOCKS_NAME": "领涨股名称",
    "LS_CHANGE_RATE": "领涨股涨跌幅",
    "INDEX_CLOSE_PRICE": "上证指数",
    "INDEX_CHANGE_RATE": "上证指数涨跌幅",
}
df_stock.rename(columns=col_name, inplace=True)
df_stock = df_stock.swaplevel()

# 提取北向资金
df_bxzj = df_stock.loc[["hk2sh", "hk2sz"], :]
df_bxzj.to_csv("北向资金.csv", encoding="gbk")
print(df_bxzj)

# 提取南向资金
df_nxzj = df_stock.loc[["sh2hk", "sz2hk"], :]
df_bxzj.to_csv("南向资金.csv", encoding="gbk")
print((df_nxzj))
