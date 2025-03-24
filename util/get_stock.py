import baostock as bs
import pandas as pd
import numpy as np
import util.mysql_util as my
import util.time_util as tu
from util.redis_util import RedisUtil


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
    if (stock_no[0:2] in '300' or stock_no[0:2] in '301' or stock_no[0:2] in '000' or stock_no[
                                                                                      0:2] in '001' or stock_no[
                                                                                                       0:2] in '002') and '.' not in stock_no:
        return stock_no + ".SZ"
    elif (stock_no[0:2] in '600' or stock_no[0:2] in '601' or stock_no[0:2] in '603' or stock_no[
                                                                                        0:2] in '605' or stock_no[
                                                                                                         0:2] in '688') and '.' not in stock_no:
        return stock_no + ".SH"
    return stock_no


# 获取股票历史数据
def get_some_stock_data(stock_no, start_date, end_date, frequency, adjust_flag="3"):
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
            'pctChg': 'increase_and_decrease'
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
            'pctChg': 'increase_and_decrease',  # 增减百分比
            'isST': 'if_st',
            'pbMRQ': 'pb_ratio'
        }).replace(r'^\s*$', None, regex=True)

    # 添加stock_id列
    df['stock_id'] = df['stock_code'].map(extract_stock_id)
    get_login_out()

    return df


def insert_batch_into_stock_price_record(frequency, df):
    if not isinstance(df, pd.DataFrame):
        return False
    # # 使用 to_sql 方法将 DataFrame 写入数据库
    table_name = 'stock_history_date_price'
    if frequency == 'w':
        table_name = 'stock_history_week_price'
    elif frequency == 'm':
        table_name = 'stock_history_month_price'

    # 创建 SQLAlchemy 引擎
    engine = my.get_mysql_connection()
    cnt = my.batch_insert_or_update(engine, df, table_name, 'stock_code', 'stock_date')
    if cnt > 0:
        return True
    else:
        return False


# 获取所有数据的产业数据
def get_stock_basic():
    update_date = tu.get_last_some_time(0)
    get_login()
    # 获取行业分类数据
    rs = bs.query_stock_basic()
    data_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)
    df = result.rename(columns={
        'code': 'stock_code',
        'code_name': 'stock_name',
        'ipoDate': 'ipo_date',
        'outDate': 'out_date',
        'type': 'stock_type',
        'status': 'stock_status'
    }).replace(r'^\s*$', None, regex=True)
    df['update_date'] = update_date
    get_login_out()
    # # 使用 to_sql 方法将 DataFrame 写入数据库
    table_name = 'stock_basic'
    # 创建 SQLAlchemy 引擎
    engine = my.get_mysql_connection()
    my.batch_insert_or_update(engine, df, table_name, 'stock_code')


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
        'code_name': 'stock_name',
        'industry': 'industry',
        'industryClassification': 'industry_classification'
    })
    get_login_out()
    # # 使用 to_sql 方法将 DataFrame 写入数据库
    table_name = 'stock_industry'
    # 创建 SQLAlchemy 引擎
    engine = my.get_mysql_connection()
    my.batch_insert_or_update(engine, df, table_name, 'stock_code')


# 生成upsert语句
def update_table_update_stock_record(stock_name, stock_code, update_column, update_stock_date):
    insert_sql = f'''
    INSERT INTO update_stock_record (stock_name,stock_code,{update_column}) VALUES('{stock_name}','{stock_code}','{update_stock_date}')
    ON DUPLICATE KEY UPDATE {update_column} = if(stock_code = values(stock_code),VALUES({update_column}),{update_column}),
    stock_name = if(stock_code = values(stock_code),VALUES(stock_name),stock_name);
    '''
    return insert_sql


def get_stock_list_for_update_sql(update_date_column=None):
    if update_date_column is not None:
        select_sql = f'''
        select b.stock_code, b.stock_name, IFNULL(a.{update_date_column}, '2000-01-01') {update_date_column}
        from (SELECT stock_code, stock_name from stock.stock_basic where stock_type = 1 and stock_status = 1) b
             left join (SELECT stock_name,stock_code,{update_date_column} from stock.update_stock_record) a
                       on a.stock_code = b.stock_code;
        '''
    else:
        select_sql = f'''
        SELECT stock_code, stock_name from stock.stock_basic where stock_type = 1 and stock_status = 1
        '''
    return select_sql


def get_stock_list_for_update_df(update_date_column=None):
    select_sql = get_stock_list_for_update_sql(update_date_column)
    engine = my.get_mysql_connection()
    result = my.execute_read_query(engine, select_sql)
    return pd.DataFrame(result)


# 获取股票代码拼接
def get_stock_code(stock_code_df):
    stock_code = stock_code_df['stock_code']
    return ', '.join([f"'{col}'" for col in stock_code])


# 更新当天数据
def update_all_stock_today_price(frequency):
    update_column = 'update_stock_date'
    if frequency == 'w':
        update_column = 'update_stock_week'
    elif frequency == 'm':
        update_column = 'update_stock_month'
    today = tu.get_last_some_time(0)
    # 先出Redis
    result_df = get_redis_update_stock_list(frequency)
    if len(result_df) == 0:
        result_df = set_redis_update_stock_list(frequency)
    engine = my.get_mysql_connection()
    st_list = ', '.join([f"'{col}'" for col in result_df['stock_code'].values])
    sql = f'''
    select stock_code,stock_name from stock_basic where stock_code in ({st_list})
    '''
    result_df = pd.DataFrame(my.execute_read_query(engine, sql))
    for i in result_df.values:
        print(f'获取<{i[1]}>股票的数据....')
        if frequency == 'd':
            df = get_some_stock_data(i[0], today, today, "d")
            flag = insert_batch_into_stock_price_record(frequency, df)
            if flag:
                insert_sql = update_table_update_stock_record(i[1], i[0], f'{update_column}', today)
                my.execute_query(engine, insert_sql)
            remove_redis_update_stock_code(frequency, i[0])
            tu.random_pause(1)
        elif frequency == 'w':
            df = get_some_stock_data(i[0], today, today, "w")
            flag = insert_batch_into_stock_price_record(frequency, df)
            if flag:
                insert_sql = update_table_update_stock_record(i[1], i[0], f'{update_column}', today)
                my.execute_query(engine, insert_sql)
            remove_redis_update_stock_code(frequency, i[0])
            tu.random_pause(1)
        elif frequency == 'm':
            df = get_some_stock_data(i[0], today, today, "m")
            flag = insert_batch_into_stock_price_record(frequency, df)
            if flag:
                insert_sql = update_table_update_stock_record(i[1], i[0], f'{update_column}', today)
                my.execute_query(engine, insert_sql)
            remove_redis_update_stock_code(frequency, i[0])
            tu.random_pause(1)


# 获取所有数据的历史数据（不包含当天数据）
def update_all_stock_history_date_week_month_price(frequency):
    last_day = tu.get_last_some_time(1)
    # 先出Redis
    result_df = get_redis_update_stock_list(frequency)
    if len(result_df) == 0:
        result_df = set_redis_update_stock_list(frequency)
    engine = my.get_mysql_connection()
    st_list = ', '.join([f"'{col}'" for col in result_df['stock_code'].values])
    sql = f'''
    select stock_code,stock_name from stock_basic where stock_code in ({st_list})
    '''
    df = pd.DataFrame(my.execute_read_query(engine, sql))
    for i in df.values:
        if i[2] <= last_day:
            print(f'获取<{i[1]}>股票的数据....')
            if frequency == 'd':
                df = get_some_stock_data(i[0], i[2], last_day, "d")
                flag = insert_batch_into_stock_price_record(frequency, df)
                if flag:
                    insert_sql = update_table_update_stock_record(i[1], i[0], 'update_stock_date', last_day)
                    my.execute_query(engine, insert_sql)
                remove_redis_update_stock_code(frequency, i[0])
                tu.random_pause(2)
            elif frequency == 'w':
                df = get_some_stock_data(i[0], i[2], last_day, "w")
                flag = insert_batch_into_stock_price_record(frequency, df)
                if flag:
                    insert_sql = update_table_update_stock_record(i[1], i[0], 'update_stock_week', last_day)
                    my.execute_query(engine, insert_sql)
                remove_redis_update_stock_code(frequency, i[0])
                tu.random_pause(2)
            elif frequency == 'm':
                df = get_some_stock_data(i[0], i[2], last_day, "m")
                flag = insert_batch_into_stock_price_record(frequency, df)
                if flag:
                    insert_sql = update_table_update_stock_record(i[1], i[0], 'update_stock_month', last_day)
                    my.execute_query(engine, insert_sql)
                remove_redis_update_stock_code(frequency, i[0])
                tu.random_pause(2)
        else:
            print(f'<{i[1]}>股票数据已更新...最新数据：{i[2]}')


def init_update_stock_record():
    stock_list = get_stock_list_for_update_df()
    engine = my.get_mysql_connection()
    my.batch_insert_or_update(engine, stock_list, 'update_stock_record', 'stock_code')


# 获取macd 和 股票价格
def get_stock_price_record_and_macd(stock_code, frequency):
    price_table = 'stock_history_date_price'
    macd_table = 'date_stock_macd'
    if frequency == 'w':
        price_table = 'stock_history_week_price'
        macd_table = 'week_stock_macd'
    elif frequency == 'm':
        price_table = 'stock_history_month_price'
        macd_table = 'month_stock_macd'

    get_stock_list_sql = f'''
    select a.stock_code, a.stock_date, open_price, high_price, low_price,
           close_price, trading_volume, trading_amount, turn, diff, macd, dea from
    (select stock_code, stock_date, open_price, high_price,
           low_price, close_price, trading_volume, trading_amount, turn
    from stock.{price_table}
    where stock_code = '{stock_code}') a
    join (select stock_code,stock_date,diff,macd,dea
    from stock.{macd_table}
        where stock_code = '{stock_code}') b
    on a.stock_date = b.stock_date
    and a.stock_code = b.stock_code;
    '''
    engine = my.get_mysql_connection()
    result = my.execute_read_query(engine, get_stock_list_sql)
    return pd.DataFrame(result)


def init_stock_profit_data(profit_year):
    engine = my.get_mysql_connection()
    stock_list = get_stock_list_for_update_df()['stock_code']
    update_columns = ['stock_code', 'performance_year', 'performance_season', 'performance_update_date',
                      'performance_type']
    profit_columns = ['code', 'pubDate', 'statDate', 'roeAvg', 'npMargin', 'gpMargin', 'netProfit', 'epsTTM',
                      'MBRevenue', 'totalShare', 'liqaShare', 'season']
    for stock_code in stock_list:
        # 登陆系统
        # 初始化空列表用于存储所有利润数据
        profit_list = []
        update_list = []
        # 查询季频估值指标盈利能力
        for j in range(2007, profit_year + 1):
            season = []
            for i in range(1, 5):
                get_login()
                rs_profit = bs.query_profit_data(code=stock_code, year=j, quarter=i)
                while (rs_profit.error_code == '0') & rs_profit.next():
                    rs = rs_profit.get_row_data()
                    if len(rs) > 0:
                        # 将季度数添加到当前行数据的末尾
                        rs.append(str(i))  # 注意这里直接修改 rs 列表
                        profit_list.append(rs)
                        season.append(str(i))
                get_login_out()
                tu.random_pause(1)
            if len(season) > 0:
                update_record = [stock_code, profit_year, ','.join(season), tu.get_last_some_time(0), 1]
                update_list.append(update_record)
            tu.random_pause(2)
        # 将所有利润数据转换为 DataFrame
        if profit_list:  # 确保有数据时才创建 DataFrame
            result_profit = pd.DataFrame(profit_list, columns=profit_columns)
            # 初始化 df 为空 DataFrame 或者直接使用 result_profit
            df = pd.DataFrame(columns=result_profit.columns) if result_profit.empty else result_profit
            df = df.rename(columns={
                'code': 'stock_code',
                'pubDate': 'publish_date',
                'statDate': 'statistic_date',
                'roeAvg': 'roe_avg',
                'npMargin': 'np_margin',
                'gpMargin': 'gp_margin',
                'netProfit': 'net_profit',
                'epsTTM': 'eps_ttm',
                'MBRevenue': 'mb_revenue',
                'totalShare': 'total_share',
                'liqaShare': 'liqa_share',
                'season': 'season'
            }).replace(r'^\s*$', None, regex=True)
            my.batch_insert_or_update(engine, df, 'stock_profit_data', 'stock_code')
        else:
            print("No data found.")

            # 登出系统
        if len(update_list) > 0:
            rf = pd.DataFrame(update_list, columns=update_columns)  # 注意这里需要将 update_record 包装成一个列表
            my.insert_or_update(engine, rf, 'stock_performance_update_record', 'stock_code', 'performance_year',
                                'performance_type')
            print(f'<{stock_code}> 已更新')


def set_redis_update_stock_list(frequency):
    column = 'update_stock_date'
    if frequency == 'w':
        column = 'update_stock_week'
    elif frequency == 'm':
        column = 'update_stock_month'
    df = get_stock_list_for_update_df(column)[['stock_code', f'{column}']]
    today = tu.get_last_some_time(0)
    rs = RedisUtil()
    for record in df.values:
        timestamp = tu.turn_date_to_timestamp(record[1])
        rs.add_sortSet(f'{today}_{column}', {f'{record[0]}': timestamp})
    return df


def get_redis_update_stock_list(frequency):
    column = 'update_stock_date'
    if frequency == 'w':
        column = 'update_stock_week'
    elif frequency == 'm':
        column = 'update_stock_month'
    rs = RedisUtil()
    today = tu.get_last_some_time(0)
    result = rs.get_sortSet_by_scoreRange(f'{today}_{column}', 0, '+inf')
    return pd.DataFrame(result, columns=['stock_code'])


def remove_redis_update_stock_code(frequency, stock_code):
    column = 'update_stock_date'
    if frequency == 'w':
        column = 'update_stock_week'
    elif frequency == 'm':
        column = 'update_stock_month'
    rs = RedisUtil()
    today = tu.get_last_some_time(0)
    return rs.delete_sortSet_by_member(f'{today}_{column}', stock_code)


if __name__ == '__main__':
    # get_stock_industry()
    # init_update_stock_record()
    # update_all_stock_history_date_week_month_price("d")
    # update_all_stock_history_date_week_month_price("m")
    # update_all_stock_history_date_week_month_price("m`")
    # update_all_stock_today_price('d')
    # init_stock_profit_data(2025)
    # print(get_some_stock_data("sz.003008", "2025-03-19", "2025-03-19", "d"))
    # print(get_redis_update_stock_list('d', ''))
    # print(remove_redis_update_stock_code('d', 'sh.600000'))
    update_all_stock_today_price('d')
