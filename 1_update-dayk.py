from WindPy import w
import pandas as pd
import os
import re
from datetime import datetime, timedelta
import time
import warnings
from Config import *

warnings.filterwarnings("ignore")
pd.set_option('display.max_columns', None)
"""
本文件使用wind_api爬取商品期货全品种全合约数据。
- 其中合约和期货品种用wset的一个功能实现；
- 获取日线数据用wsd实现；
- 
"""

def get_day_K_function(code, time_start, time_end):
    """
    获取日线数据，并进行改名
    :param code:
    :param time_start:
    :param time_end:
    :return:
    """
    data = w.wsd(code,
                 "pre_close,open,high,low,close,chg,pct_chg,volume,amt,pre_settle,settle,oi,oi_chg,sccode,trade_hiscode",
                 time_start, time_end, unit=1)
    data = pd.DataFrame(data.Data, index=data.Fields, columns=data.Times).T
    data.reset_index(inplace=True)
    data.rename(columns={'index': '交易日期'}, inplace=True)
    data.rename(columns={
        'PRE_CLOSE': '前收盘价',
        'OPEN': '开盘价',
        'HIGH': '最高价',
        'LOW': '最低价',
        'CLOSE': '收盘价',
        'CHG': '涨跌',
        'PCT_CHG': '涨跌幅',
        'VOLUME': '成交量',
        'AMT': '成交额',
        'PRE_SETTLE': '前结算价',
        'SETTLE': '结算价',
        'OI': '持仓量',
        'OI_CHG': '持仓变动',
        'TRADE_HISCODE': '合约代码'
    }, inplace=True)
    data['成交额'] = data['成交额'] / 10000
    return data

def extract_chinese_name(sec_name):
    # 使用正则表达式匹配所有非数字字符直到字符串结束
    match = re.match(r'^(.*?)(\d+)$', sec_name)
    if match:
        # 返回匹配的中文部分
        return match.group(1)
    else:
        # 如果没有匹配到数字，则说明整个字符串都是名称
        return sec_name

w.start()  # 默认命令超时时间为120秒，如需设置超时时间可以加入waitTime参数，例如waitTime=60,即设置命令超时时间为60秒

w.isconnected()

base_path = 'D:\data\\future\day_k'
explanation_text = "私人整理，请勿传播"  # 第一行放的文本
today_date = datetime.now()  # 设置今天的日期
ten_days_before = today_date - timedelta(days=10)  # 从今天减去10天得到新的日期
end_time = today_date.strftime('%Y-%m-%d')
start_time = ten_days_before.strftime('%Y-%m-%d')
start_timedelta = time.time()

# 获取期货合约代码
contract_code = w.wset("sectorconstituent", date=today_date, sectorid=1000015512000000)
contract_code = pd.DataFrame(contract_code.Data, index=contract_code.Fields).T
list_code_contract = contract_code['wind_code'].tolist()
print(contract_code)
contract_code.to_csv(base_path + '\code.csv', encoding='gbk')

# 爬取日K数据
for index, row in contract_code.iterrows():
    wind_code = row['wind_code']
    sec_name = row['sec_name']
    print(sec_name)
    # ==== 爬取日度K线数据
    daily_k_data = get_day_K_function(wind_code, start_time, end_time)

    # ==== 路径
    # 创建子目录路径
    sub_directory = os.path.join(base_path, extract_chinese_name(sec_name))
    if not os.path.exists(sub_directory):
        os.makedirs(sub_directory)  # 如果子目录不存在就创建一个

    # 文件名和目录可能不一致
    file_path = os.path.join(sub_directory, f"{sec_name}.csv")

    # ==== 存储
    if os.path.exists(file_path):
        # 文件已存在，读取原始数据并与新数据合并
        historical_data = pd.read_csv(file_path, skiprows=1, parse_dates=['交易日期'], encoding='gbk')
        historical_data['交易日期'] = pd.to_datetime(historical_data['交易日期'])
        daily_k_data['交易日期'] = pd.to_datetime(daily_k_data['交易日期'])

        # 合并旧数据和新数据
        combined_data = pd.concat([historical_data, daily_k_data]).drop_duplicates(['交易日期'])

        # 再排序一次
        combined_data.sort_values('交易日期', inplace=True)

        # 保存或覆盖CSV文件
        file_title = pd.DataFrame(columns=[explanation_text])
        # combined_data.to_csv(file_path, encoding='gbk', index=False, mode='a', header=False)
        file_title.to_csv(file_path, index=False, encoding='GBK', mode='w')
        combined_data.to_csv(file_path, index=False, mode='a', encoding='GBK')
    else:
        # 文件不存在，直接使用新数据
        combined_data = daily_k_data
        combined_data.sort_values('交易日期', inplace=True)
        combined_data.drop_duplicates(subset=['交易日期'], inplace=True) # 任何数据都去重排序，以防万一
        # 将说明文本和数据写入文件
        with open(file_path, 'a', encoding='gbk') as f:
            f.write(explanation_text + '\n')  # 写入说明文本并换行
            combined_data.to_csv(f, index=False, encoding='gbk')

elapsed_time = time.time() - start_timedelta

print(f"The code took {elapsed_time} seconds to run.")
w.stop()
