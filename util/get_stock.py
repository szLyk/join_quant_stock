import baostock as bs
import pandas as pd
import numpy as np
from util.mysql_util import insert_or_update, execute_read_query, get_mysql_connection, execute_query
from util.time_util import get_last_some_time, random_pause


# 登陆
def get_login():
    login = bs.login()
    error_code = login.error_code
    if error_code != "0":
        raise Exception("登录失败！")


# 登出
def get_login_out():
    bs.logout()


# 定义一个函数来提取数字部分
def extract_stock_id(stock_code):
    return stock_code.split('.')[-1]


def fix_stock_code(stock_no):
    if (stock_no[0:2] in '300' or stock_no[0:2] in '301' or stock_no[0:2] in '000' or stock_no[0:2] in '001' or stock_no[0:2] in '002') and '.' not in stock_no:
        return stock_no + ".SZ"
    elif (stock_no[0:2] in '600' or stock_no[0:2] in '601' or stock_no[0:2] in '603' or stock_no[0:2] in '605' or stock_no[0:2] in '688') and '.' not in stock_no:
        return stock_no + ".SH"
    return stock_no


# 获取股票历史数据
def get_some_stock_data(stock_no, start_date, end_date, frequency, adjust_flag="2"):
    get_login()
    stock_no = fix_stock_code(stock_no)
    column = "date,code,open,high,low,close,volume,amount,adjustflag,turn,tradestatus,pctChg,isST,pbMRQ"
    if frequency == 'w' or frequency == 'm':
        column = "date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg"
    rs = bs.query_history_k_data_plus(stock_no,
                                      column,
                                      start_date=start_date, end_date=end_date,
                                      frequency=frequency, adjustflag=adjust_flag)

    data_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())

    if len(data_list) == 0:
        print(f'查询 <{stock_no}> 无数据,返回。')
        get_login_out()
        return False

    result = pd.DataFrame(data_list, columns=rs.fields)
    df = None
    if frequency == 'w' or frequency == 'm':
        df = result.rename(columns={
            'date': 'stock_date',
            'code': 'stock_code',
            'open': 'open_price',
            'high': 'high_price',
            'low': 'low_price',
            'close': 'close_price',
            'volume': 'trading_volume',
            'amount': 'trading_amount',
            'adjustflag': 'adjust_flag',
            'turn': 'turn',
            'pctChg': 'Increase_and_decrease'
        }).replace(r'^\s*$', None, regex=True)
    elif frequency == 'd':
        # 重命名列
        df = result.rename(columns={
            'date': 'stock_date',
            'code': 'stock_code',
            'open': 'open_price',
            'high': 'high_price',
            'low': 'low_price',
            'close': 'close_price',
            'volume': 'trading_volume',
            'amount': 'trading_amount',
            'adjustflag': 'adjust_flag',
            'turn': 'turn',
            'tradestatus': 'tradestatus',
            'pctChg': 'Increase_and_decrease',  # 增减百分比
            'isST': 'if_st',
            'pbMRQ': 'pb_ratio'
        }).replace(r'^\s*$', None, regex=True)

    # 添加stock_id列
    df['stock_id'] = df['stock_code'].map(extract_stock_id)
    get_login_out()

    # # 使用 to_sql 方法将 DataFrame 写入数据库
    table_name = 'stock_history_date_price'
    if frequency == 'w':
        table_name = 'stock_history_week_price'
    elif frequency == 'm':
        table_name = 'stock_history_month_price'

    # 创建 SQLAlchemy 引擎
    engine = get_mysql_connection()
    cnt = insert_or_update(engine, df, table_name, 'stock_code', 'stock_date')
    if cnt > 0:
        return True
    else:
        return False


# 获取所有数据的产业数据
def get_stock_industry():
    get_login()
    # 获取行业分类数据
    rs = bs.query_stock_industry()
    data_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)
    df = result.rename(columns={
        'updateDate': 'update_date',
        'code': 'stock_code',
        'code_name': 'code_name',
        'industry': 'industry',
        'industryClassification': 'industry_classification'
    })
    get_login_out()
    # # 使用 to_sql 方法将 DataFrame 写入数据库
    table_name = 'stock_industry'
    # 创建 SQLAlchemy 引擎
    engine = get_mysql_connection()
    insert_or_update(engine, df, table_name, 'stock_code')


def update_table_update_stock_record(stock_name, stock_code, update_column, update_stock_date):
    insert_sql = f'''
    INSERT INTO update_stock_record (stock_name,stock_code,{update_column}) VALUES('{stock_name}','{stock_code}','{update_stock_date}')
    ON DUPLICATE KEY UPDATE {update_column} = if(stock_code = values(stock_code),VALUES({update_column}),{update_column}),
    stock_name = if(stock_code = values(stock_code),VALUES(stock_name),stock_name);
    '''
    return insert_sql


def get_stock_list(update_date):
    select_sql = f'''
    select b.stock_code, b.code_name, IFNULL(a.{update_date}, '2000-01-01') {update_date}
    from (SELECT stock_code, code_name from stock_industry ) b
         left join (SELECT stock_name,stock_code,{update_date} from update_stock_record) a
                   on a.stock_code = b.stock_code;
    '''
    return select_sql


# 获取所有数据的历史数据（不包含当天数据）
def update_all_stock_history_date_price():
    engine = get_mysql_connection()
    select_sql = get_stock_list('update_stock_date')
    last_day = get_last_some_time(1)
    result = execute_read_query(engine, select_sql)
    df = pd.DataFrame(result)
    for i in df.values:
        if i[2] <= last_day:
            print(f'获取<{i[1]}>股票的数据....')
            flag = get_some_stock_data(i[0], i[2], last_day, "d")
            if flag:
                insert_sql = update_table_update_stock_record(i[1], i[0], 'update_stock_date', i[2])
                execute_query(engine, insert_sql)
                random_pause(5)
        else:
            print(f'<{i[1]}>股票数据已更新...最新数据：{i[2]}')


# 更新当天数据
def update_all_stock_today_price():
    engine = get_mysql_connection()
    select_sql = get_stock_list('update_stock_date')
    today = get_last_some_time(0)
    result = execute_read_query(engine, select_sql)
    df = pd.DataFrame(result)

    for i in df.values:
        print(f'获取<{i[1]}>股票的数据....')
        flag = get_some_stock_data(i[0], today, today, "d")
        if flag:
            insert_sql = update_table_update_stock_record(i[1], i[0], 'update_stock_date', today)
            execute_query(engine, insert_sql)
            random_pause(2)


# 获取所有数据的历史数据（不包含当天数据）
def update_all_stock_history_date_week_month_price(frequency):
    engine = get_mysql_connection()
    select_sql = get_stock_list('update_stock_date')
    if frequency == 'w':
        select_sql = get_stock_list('update_stock_week')
    elif frequency == 'm':
        select_sql = get_stock_list('update_stock_month')
    last_day = get_last_some_time(1)
    result = execute_read_query(engine, select_sql)
    df = pd.DataFrame(result)
    for i in df.values:
        if i[2] <= last_day:
            print(f'获取<{i[1]}>股票的数据....')
            if frequency == 'd':
                flag = get_some_stock_data(i[0], i[2], last_day, "d")
                if flag:
                    insert_sql = update_table_update_stock_record(i[1], i[0], 'update_stock_date', last_day)
                    execute_query(engine, insert_sql)
                    random_pause(5)
            elif frequency == 'w':
                flag = get_some_stock_data(i[0], i[2], last_day, "w")
                if flag:
                    insert_sql = update_table_update_stock_record(i[1], i[0], 'update_stock_week', last_day)
                    execute_query(engine, insert_sql)
                    random_pause(3)
            elif frequency == 'm':
                flag = get_some_stock_data(i[0], i[2], last_day, "m")
                if flag:
                    insert_sql = update_table_update_stock_record(i[1], i[0], 'update_stock_month', last_day)
                    execute_query(engine, insert_sql)
                    random_pause(2)
        else:
            print(f'<{i[1]}>股票数据已更新...最新数据：{i[2]}')


if __name__ == '__main__':
    # get_some_stock_data("301618", "2025-03-10", "2025-03-11", "d")
    # get_stock_industry()
    # update_all_stock_history_date_price()
    update_all_stock_today_price()
    # update_all_stock_history_date_week_month_price("m")
