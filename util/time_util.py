from datetime import datetime, timedelta
import random
import time
import holidays
import pandas as pd
import pytz


def get_last_some_time(day):
    # 获取当前时间
    current_time = datetime.now()
    # 减去一天
    previous_day = current_time - timedelta(days=day)
    return previous_day.strftime('%Y-%m-%d')


def random_pause(max_seconds=10):
    # 生成一个从0到max_seconds的随机浮点数
    pause_time = random.uniform(0, max_seconds)
    print(f"程序将暂停 {pause_time:.2f} 秒")
    # 让程序暂停pause_time秒
    time.sleep(pause_time)


def is_trading_day(date):
    # 创建中国节假日实例
    cn_holidays = holidays.CountryHoliday('CN', years=[date.year])  # 指定年份以提高效率
    """ 判断给定日期是否为交易日（非周末且非节假日） """
    if date.weekday() < 5 and date not in cn_holidays:
        return True
    return False


def find_last_trading_day_of_month(given_date):
    date = pd.to_datetime(given_date)
    """ 找到给定日期所在月份的最后一天，并返回该月最后一个交易日 """
    last_day_of_month = (date + pd.offsets.MonthEnd(0)).date()

    while not is_trading_day(last_day_of_month):
        last_day_of_month -= pd.Timedelta(days=1)

    return last_day_of_month


def find_last_trading_day_of_week(given_date):
    date = pd.to_datetime(given_date)
    # 创建中国节假日实例
    cn_holidays = holidays.CountryHoliday('CN', years=[date.year])  # 指定年份以提高效率
    """ 找到给定日期所在周的周五，并返回该周最后一个交易日 """
    # 计算给定日期所在周的周五
    friday_of_week = date + pd.offsets.Week(weekday=4) - pd.offsets.Week()
    if friday_of_week > date:
        friday_of_week -= pd.offsets.Week()

    # 确保我们从包含给定日期的那一周的周五开始
    friday_of_week += pd.offsets.Week()

    # 向前查找最近的交易日
    current_day = friday_of_week
    while current_day.weekday() > 4 or current_day in cn_holidays:  # 如果是周末或节假日
        current_day -= pd.Timedelta(days=1)

    return current_day.date()


def find_first_trading_day_of_week(given_date):
    """
    给定一个日期，返回该日期所在周的第一个交易日。

    参数:
        given_date (str or datetime): 输入的日期。

    返回:
        date: 该周的第一个交易日。
    """
    # 将输入日期转换为datetime对象
    date = pd.to_datetime(given_date)

    # 创建中国节假日实例
    cn_holidays = holidays.CountryHoliday('CN', years=[date.year])  # 指定年份以提高效率

    # 计算给定日期所在周的周一
    monday_of_week = date - pd.offsets.Day(date.weekday())  # weekday()返回0表示周一

    # 向前查找最近的交易日
    current_day = monday_of_week
    while current_day.weekday() > 4 or current_day in cn_holidays:  # 如果是周末或节假日
        current_day += pd.Timedelta(days=1)  # 如果是周末或假日，则跳过这一天

    return current_day.date()


def turn_date_to_timestamp(date_str):
    # 定义日期字符串的格式
    date_format = '%Y-%m-%d'
    # 将日期字符串转换为datetime对象
    dt_obj = datetime.strptime(date_str, date_format)
    # 将datetime对象转换为时间戳（秒）
    return dt_obj.timestamp()


def turn_time_to_timestamp(date_str):
    # 定义日期字符串的格式
    date_format = '%Y-%m-%d %H:%M:%S'
    # 将日期字符串转换为datetime对象
    dt_obj = datetime.strptime(date_str, date_format)
    # 将datetime对象转换为时间戳（秒）
    return dt_obj.timestamp()


def turn_timestamp_to_date(timestamp, formatted):
    # 转换为UTC时间
    utc_time = datetime.utcfromtimestamp(timestamp)
    # 定义目标时区
    target_timezone = pytz.timezone('Asia/Shanghai')
    # 转换为指定时区的时间
    target_time = utc_time.replace(tzinfo=pytz.utc).astimezone(target_timezone)
    format_time = '%Y-%m-%d %H:%M:%S %Z'
    if formatted == 'd':
        return target_time.strftime("%Y-%m-%d")
    # 格式化时间
    return target_time.strftime(format_time)


if __name__ == '__main__':
    # 示例调用
    given_date = '2025-03-17'
    first_trading_day = find_first_trading_day_of_week(given_date)
    last_trading_day = find_last_trading_day_of_week(given_date)
    print("本周的第一个交易日是:", first_trading_day)
    print("本周的最后个交易日是:", last_trading_day)
