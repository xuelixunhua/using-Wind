from WindPy import w
import pandas as pd
import os
import re
from datetime import datetime, timedelta
import time
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed


warnings.filterwarnings("ignore")
pd.set_option('display.max_columns', None)
"""
本文件使用wind_api爬取商品期货全品种全合约持仓数据。
- 其中合约和期货品种用wset的一个功能实现；
- 获取持仓数据用wset实现；
- 
"""


def get_basic_data_by_field(code, time_start, field, way):
    """
    获取每次的基础数据
    :param code:
    :param time_start: 日期
    :param field: 需要获取的字段
    :return:
    """
    fields = ["date", "ranks", "member_name", field, f"{field}_increase"]
    if field == 'vol':
        data = w.wset("futurevir",f"startdate={time_start};enddate={time_start};wind_code={code};"
                                  f"ranks=all;field={','.join(fields)}")
    else:
        data = w.wset("openinterestranking",
                      f"startdate={time_start};enddate={time_start};wind_code={code};"
                      f"order_by={way};ranks=all;field={','.join(fields)}")
    if data.ErrorCode != 0:
        print(f"Error fetching data for {field}: {data}")
        return None
    if not data.Data:
        print(f"No data returned for {field} with code {code} on {time_start}")
        return None
    # print(data)
    df = pd.DataFrame(data.Data, index=data.Fields, columns=data.Data[0]).T

    if df.empty:
        print(f"No data returned for {field} with code {code} on {time_start}")
        return None
    return df


def save_combined_data_for_variety(variety, date, list_code_contract):
    combined_df_list = []

    for contract_code_temp in list_code_contract:
        if extract_variety_code(contract_code_temp) == variety:
            print(contract_code_temp)
            df_vol = get_basic_data_by_field(contract_code_temp, date, "vol", 'long')
            df_long = get_basic_data_by_field(contract_code_temp, date, "long_position", 'long')
            df_short = get_basic_data_by_field(contract_code_temp, date, "short_position", 'short')

            # 检查是否有数据缺失
            if df_vol is None or df_long is None or df_short is None:
                print(f"Data missing for {contract_code_temp} on {date}, skipping.")
                continue

            # 重置索引以避免重复的索引问题
            df_vol.reset_index(drop=True, inplace=True)
            df_long.reset_index(drop=True, inplace=True)
            df_short.reset_index(drop=True, inplace=True)
            # 合并数据
            try:
                df = pd.concat([df_vol, df_long.drop(columns=['date']),
                                df_short.drop(columns=['date'])], axis=1)
            except pd.errors.InvalidIndexError as e:
                print(f"Error concatenating data for {contract_code_temp} on {date}: {e}")
                continue

            # 添加合约代码列
            df['合约代码'] = contract_code_temp

            # 添加到列表中
            combined_df_list.append(df)

    # 合并所有合约的DataFrame
    try:
        combined_df = pd.concat(combined_df_list, ignore_index=True)
    except ValueError as e:
        print(f"Error concatenating data for {contract_code_temp} on {date}: {e}")
        return None

    # 重命名列以匹配目标表格
    combined_df.columns = ['日期', '名次', '会员简称_成交量', '成交量', '成交量增减',
                           '名次_', '会员简称_持买单量', '持买单量', '持买单量增减',
                           '名次__', '会员简称_持卖单量', '持卖单量', '持卖单量增减', '合约代码']

    # 删除多余的名次列
    combined_df.drop(columns=['名次_', '名次__'], inplace=True)

    # 保存到文件
    save_path = f"{base_path}\\{variety}.csv"
    combined_df.to_csv(save_path, index=False, encoding='utf-8-sig')
    print(f"Data saved to {save_path}")


def extract_variety_code(contract):
    # 提取品种代码的正则表达式，匹配字母开头，后跟数字，然后是交易所标识
    match = re.match(r"([a-zA-Z]+)\d+\.\w+", contract)
    if match:
        return match.group(1).upper()  # 返回大写的品种代码
    return None


base_path = 'D:\data\\future\exchange_day'
explanation_text = "私人整理，请勿传播"  # 第一行放的文本

def main():
    w.start()  # 默认命令超时时间为120秒，如需设置超时时间可以加入waitTime参数，例如waitTime=60,即设置命令超时时间为60秒
    w.isconnected()


    today_date = datetime.now() # 设置今天的日期
    end_time = today_date.strftime('%Y-%m-%d')
    start_timedelta = time.time()

    try:
        # 获取期货合约代码
        contract_code_1 = w.wset("sectorconstituent", date=today_date, sectorid=1000015512000000)
        contract_code_2 = w.wset("sectorconstituent", date=today_date, sectorid='a599010101000000')
        contract_code1_df = pd.DataFrame(contract_code_1.Data, index=contract_code_1.Fields).T
        contract_code2_df = pd.DataFrame(contract_code_2.Data, index=contract_code_2.Fields).T
        contract_code = pd.concat([contract_code1_df, contract_code2_df], ignore_index=True)
        list_code_contract = contract_code['wind_code'].tolist()
        print(contract_code)
        # 分类提取所有品种代码
        variety_codes = set(
            extract_variety_code(contract) for contract in list_code_contract if extract_variety_code(contract))
        print(variety_codes)

        with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = [executor.submit(save_combined_data_for_variety, variety, end_time, list_code_contract) for variety in variety_codes]
            for future in as_completed(futures):
                try:
                    result = future.result()
                    print(result)
                except Exception as exc:
                    print(f"Exception during processing: {exc}")

    finally:
        elapsed_time = time.time() - start_timedelta
        print(f"The code took {elapsed_time} seconds to run.")
        w.stop()

if __name__ == "__main__":
    main()
