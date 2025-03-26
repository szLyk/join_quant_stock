import datetime
import math

from mysql.connector import Error

import util.mysql_util as my
import pandas as pd
import util.time_util as tu
import util.get_stock as gs
import util.file_util as fu
import numpy as np
from scipy.signal import argrelextrema
import matplotlib.pyplot as plt


# 获取要计算的股票数据
def calculate_stock_ma(frequency, if_init=False, batch_size=20):
    table = 'stock_history_date_price'
    column = 'update_stock_date_ma'
    moving_table = 'date_stock_moving_average_table'
    trade_status = 'and tradestatus = 1'
    sub_month = 24

    if frequency == 'm':
        table = 'stock_history_month_price'
        column = 'update_stock_month_ma'
        moving_table = 'month_stock_moving_average_table'
        trade_status = ''
        sub_month = 360
    elif frequency == 'w':
        table = 'stock_history_week_price'
        column = 'update_stock_week_ma'
        moving_table = 'week_stock_moving_average_table'
        trade_status = ''
        sub_month = 120

    get_stock_start_date = f'''
    select stock_code, update_date, min_stock_date,
        if(datediff(update_date,min_stock_date)>365*5,1,0) sub_date_diff
    from
    (SELECT a.stock_code,if(a.max_stock_date > b.{column},b.{column},a.max_stock_date) update_date,min_stock_date from
    (SELECT a.stock_code,max(a.stock_date) max_stock_date,min(a.stock_date) min_stock_date from stock.{table} a GROUP BY a.stock_code) a
    join (SELECT b.stock_code,b.{column} from stock.update_stock_record b) b on a.stock_code = b.stock_code
    join (select * from stock.stock_basic where stock_type = 1 and stock_status = 1) c on c.stock_code = a.stock_code) a;
   '''

    conn = my.get_mysql_connection()
    stock_start_date_result = my.execute_read_query(conn, get_stock_start_date)
    stock_start_date_df = pd.DataFrame(stock_start_date_result,
                                       columns=['stock_code', 'update_date', 'min_stock_date', 'sub_date_diff'])

    try:
        for i in range(0, len(stock_start_date_df), batch_size):
            batch_df = stock_start_date_df.iloc[i:i + batch_size]

            # 构建条件字符串
            conditions_one_list = []
            for record in batch_df.values:
                stock_code = record[0]
                update_date = record[1]
                sub_date_diff = record[3]
                if frequency == 'd' and sub_date_diff == 1:
                    conditions_one = f"(stock_code = '{stock_code}' and stock_date >= DATE_SUB('{update_date}'," \
                                     f" INTERVAL {sub_month} MONTH) {trade_status})"
                elif frequency == 'w' and sub_date_diff == 1:
                    conditions_one = f"(stock_code = '{stock_code}' and stock_date >= DATE_SUB('{update_date}', " \
                                     f"INTERVAL {sub_month} MONTH) {trade_status})"
                elif sub_date_diff == 0 or frequency == 'm':
                    conditions_one = f"(stock_code = '{stock_code}')"
                conditions_one_list.append(conditions_one)

            conditions_one_str = " or ".join(conditions_one_list)
            conditions_one_str = "where" + conditions_one_str
            if if_init:
                conditions_one_str = ""
            calculate_sql = f'''
                            SELECT a.stock_code,
                                stock_name, 
                                stock_date,
                                close_price
                                FROM		
                                (SELECT
                                    stock_code,
                                    stock_date,
                                    close_price
                                 from {table} 
                                 {conditions_one_str}
                                 ) a
                                         join
                                         (SELECT stock_code,stock_name from update_stock_record where stock_code in 
                                         ({", ".join([f"'{code}'" for code in batch_df['stock_code']])})) b 
                                         on a.stock_code = b.stock_code
                        '''
            result = my.execute_read_query(conn, calculate_sql)
            df = pd.DataFrame(result, columns=['stock_code', 'stock_name', 'stock_date', 'close_price'])
            # 定义均线周期
            ma_windows = [3, 5, 6, 7, 9, 10, 12, 20, 24, 26, 30, 60, 70, 125, 250]

            # 计算移动平均线
            def compute_ma(group):
                group = group.sort_values('stock_date')
                close = group['close_price'].astype(float)
                for window in ma_windows:
                    col_name = f'stock_ma{window}'
                    group[col_name] = close.rolling(window=window).mean()
                return group

            ma_result = df.groupby('stock_code')[['stock_code', 'stock_date', 'close_price']].apply(
                compute_ma).reset_index(level=0, drop=True)
            ma_result = ma_result[['stock_code', 'stock_date', 'close_price'] + [f'stock_ma{w}' for w in ma_windows]]
            if not if_init:
                ma_result = ma_result.dropna(subset=['stock_ma250'])
            if len(ma_result) > 0:
                cnt = my.batch_insert_or_update(conn, ma_result, moving_table, "stock_code", "stock_date")
                if cnt > 0:
                    # 批量更新 update_stock_record 表
                    update_records = []
                    for stock_code in batch_df['stock_code']:
                        get_max_update_date_sql = f'''
                                                   select stock_code, max(stock_date) max_stock_date from {table} 
                                                   where stock_code = '{stock_code}' group by stock_code
                                                   '''
                        max_update = my.execute_read_query(conn, get_max_update_date_sql)
                        if max_update:
                            max_stock_date = max_update[0][1]
                            update_record_column = 'update_stock_date_ma'
                            if frequency == 'w':
                                update_record_column = 'update_stock_week_ma'
                            if frequency == 'm':
                                update_record_column = 'update_stock_month_ma'
                            update_records.append((max_stock_date, stock_code, update_record_column))

                    # 构建批量更新的 SQL 语句
                    update_sql = f'''
                                   UPDATE stock.update_stock_record
                                   SET {update_record_column} = CASE stock_code
                                   {"".join([f"WHEN '{record[1]}' THEN '{record[0]}'" for record in update_records])}
                                   END
                                   WHERE stock_code IN ({", ".join([f"'{record[1]}'" for record in update_records])})
                                   '''
                    my.execute_query(conn, update_sql)
    except Error as e:
        print(f"查询执行失败: {e}")


# 获取要计算的股票的MACD
def calculate_stock_macd(frequency):
    batch_size = 5
    engine = my.get_mysql_connection()
    select_table = 'stock_history_date_price'
    insert_table = 'date_stock_macd'
    days = 7
    update_macd_column = 'update_stock_date_macd'
    if frequency == 'w':
        select_table = 'stock_history_week_price'
        insert_table = 'week_stock_macd'
        update_macd_column = 'update_stock_week_macd'
        days = 70
    if frequency == 'm':
        select_table = 'stock_history_month_price'
        insert_table = 'month_stock_macd'
        update_macd_column = 'update_stock_month_macd'
        days = 700

    stock_list = gs.get_stock_list_for_update_df()
    for i in range(0, len(stock_list), batch_size):
        batch_df = stock_list.iloc[i:i + batch_size]
        stock_code = batch_df['stock_code']
        result_string = ', '.join([f"'{value}'" for value in stock_code])

        stock_date_price_sql = f'''
        SELECT
            b.stock_code,
            a.stock_name,
            b.stock_date,
            b.close_price
        FROM
            stock.{select_table} b
            JOIN (SELECT stock_name, stock_code FROM stock.stock_basic
            where stock_code in ({result_string}) and stock_type = 1 and stock_status = 1) a
            ON a.stock_code = b.stock_code
			JOIN (SELECT {update_macd_column},stock_code FROM stock.update_stock_record
			where stock_code in ({result_string})) c
            ON c.stock_code = b.stock_code and b.stock_date >= DATE_SUB( c.{update_macd_column},INTERVAL {days} day)
        '''
        stock_date_close_price = my.execute_read_query(engine, stock_date_price_sql)
        stock_df = pd.DataFrame(stock_date_close_price)
        stock_df['close_price'] = stock_df['close_price'].astype(float)
        # 初始化平滑因子
        alpha_12 = 2 / 13
        alpha_26 = 2 / 27
        alpha_9 = 2 / 10

        def compute_macd(group):
            df = group.sort_values(by='stock_date', ascending=True).reset_index(drop=True)
            first_stock_code = df.loc[0, 'stock_code']
            first_stock_date = df.loc[0, 'stock_date']
            get_ema_value = f'''
            select stock_code,stock_date,ema_12,ema_26,dea,diff,macd
             from stock.{insert_table} where stock_code = '{first_stock_code}'
            and stock_date = '{first_stock_date}'
            '''
            ma_value = my.execute_read_query(engine, get_ema_value)
            # 使用SMA初始化EMA
            if len(ma_value) == 0:
                df['ema_12'] = df['close_price'].iloc[0]
                df['ema_26'] = df['close_price'].iloc[0]
                df.loc[0, 'dea'] = 0
                df.loc[0, 'diff'] = 0
                df.loc[0, 'macd'] = 0
            else:
                df.loc[0, 'ema_12'] = float(ma_value[0][2])
                df.loc[0, 'ema_26'] = float(ma_value[0][3])
                df.loc[0, 'dea'] = float(ma_value[0][4])
                df.loc[0, 'diff'] = float(ma_value[0][5])
                df.loc[0, 'macd'] = float(ma_value[0][6])

            # 计算EMA12和EMA26
            for i in range(1, len(df)):
                df.loc[i, 'ema_12'] = alpha_12 * df.loc[i, 'close_price'] + (1 - alpha_12) * df.loc[i - 1, 'ema_12']
                df.loc[i, 'ema_26'] = alpha_26 * df.loc[i, 'close_price'] + (1 - alpha_26) * df.loc[i - 1, 'ema_26']
                df.loc[i, 'diff'] = df.loc[i, 'ema_12'] - df.loc[i, 'ema_26']
                df.loc[i, 'dea'] = alpha_9 * df.loc[i, 'diff'] + (1 - alpha_9) * df.loc[i - 1, 'dea']

            # # 计算DIFF和DEA(MACD线)
            # df['diff'] = df['ema_12'] - df['ema_26']
            # df['dea'] = df['diff'].ewm(span=9, adjust=False).mean()  # 使用pandas的ewm方法计算DEA
            # # alpha_9 * Decimal(df.loc[i, 'diff']) + (Decimal('1') - alpha_9) * Decimal(df.loc[i - 1, 'dea'])
            df['macd'] = (df['diff'] - df['dea']) * 2  # MACD是DIFF和DEA的差值乘以2

            return df

        # 对每个股票的数据进行分组计算MACD，并确保数据按日期排序
        macd_df = stock_df.groupby('stock_code').apply(compute_macd).reset_index(level=0, drop=True)

        cnt = my.batch_insert_or_update(engine, macd_df, insert_table, 'stock_code')
        # 更新记录
        if cnt > 0:
            trade_date_sql = f'''
            select stock_code,max(stock_date) as {update_macd_column} from stock.{select_table}
            where stock_code in ({result_string})
            group by stock_code
            '''
            result = my.execute_read_query(engine, trade_date_sql)
            my.insert_or_update(engine, pd.DataFrame(result), 'update_stock_record', 'stock_code')
            print(f'<{result_string} macd更新完毕....>')


# 计算当前未完整月份的收、开盘价以及涨跌幅度
def calculate_stock_month_price():
    calculate_sql = f'''
    with month_open_price as (
    SELECT  a.stock_code,a.open_price,a.close_price,
    c.close_price last_close_price
    from stock.stock_history_date_price a 
    join (
    SELECT min(stock_date) stock_date,stock_code
    from stock.stock_history_date_price 
    where date_format(stock_date,'%Y-%m') = date_format(CURRENT_DATE,'%Y-%m')
    GROUP BY stock_code
    ) b on a.stock_date = b.stock_date 
    and a.stock_code = b.stock_code
    join (
    SELECT close_price,stock_code
    from stock.stock_history_month_price 
    where date_format(stock_date,'%Y-%m') = date_format(DATE_SUB(CURRENT_TIME,INTERVAL 1 month),'%Y-%m')
    ) c on a.stock_code = c.stock_code
    ),
     month_close_price as (
    SELECT a.stock_code,a.close_price,a.stock_date
    from stock.stock_history_date_price a 
    join (
    SELECT max(stock_date) stock_date,stock_code
    from stock.stock_history_date_price 
    where date_format(stock_date,'%Y-%m') = date_format(CURRENT_DATE,'%Y-%m')
    GROUP BY stock_code
    ) b on a.stock_date = b.stock_date 
    and a.stock_code = b.stock_code)
    select a.stock_code,SUBSTRING_INDEX(a.stock_code,'.',-1) stock_id ,
        c.stock_date,b.open_price,a.high_price,a.low_price,
        c.close_price,a.trading_volume,a.trading_amount,
        3 as adjust_flag,turn,round(((c.close_price - last_close_price)/last_close_price) * 100,4) increase_and_decrease
    from 
    (SELECT stock_code,sum(trading_volume) trading_volume ,
    sum(trading_amount)trading_amount,min(low_price)low_price,
    max(high_price) high_price,sum(turn) turn
    from stock.stock_history_date_price 
    where date_format(stock_date,'%Y-%m') = date_format(CURRENT_DATE,'%Y-%m')
    GROUP BY stock_code) a join 
    month_open_price b on a.stock_code = b.stock_code
    join
    month_close_price c on a.stock_code = c.stock_code;    
    '''
    engine = my.get_mysql_connection()
    result = my.execute_read_query(engine, calculate_sql)
    df = pd.DataFrame(result)
    if len(df) == 0:
        return
    df['stock_date'] = df['stock_date'].map(tu.find_last_trading_day_of_month)
    placeholders = gs.get_stock_code(df)

    cnt = my.batch_insert_or_update(engine, df, 'stock_history_month_price', 'stock_code', 'stock_date')
    if cnt > 0:
        now_date = tu.get_last_some_time(0)
        sql = f'''
        update update_stock_record set update_stock_month = '{now_date}' where stock_code in ({placeholders});
        '''
        my.execute_query(engine, sql)


# 计算当前未完整周的收、开盘价以及涨跌幅度
def calculate_stock_week_price():
    today = tu.get_last_some_time(0)
    # 获取这周头一天的交易日
    first_trade_week = tu.find_first_trading_day_of_week(today)
    last_trade_week = tu.find_last_trading_day_of_week(today)
    last_week_trade_day = tu.find_last_trading_day_of_week(tu.get_last_some_time(7))
    calculate_sql = f'''
        with week_open_price as (
        SELECT  a.stock_code,a.open_price,a.close_price,
        c.close_price last_close_price
        from stock.stock_history_date_price a
        join (
        SELECT min(stock_date) stock_date,stock_code
        from stock.stock_history_date_price
        where  date_format(stock_date,'%Y-%m-%d') >= '{first_trade_week}'
        and date_format(stock_date,'%Y-%m-%d') <= '{last_trade_week}'
        GROUP BY stock_code
        ) b on a.stock_date = b.stock_date
        and a.stock_code = b.stock_code
        join (
        SELECT close_price,stock_code
        from stock.stock_history_week_price
        where date_format(stock_date,'%Y-%m-%d') = '{last_week_trade_day}'
        ) c on a.stock_code = c.stock_code
        ),
        week_close_price as (
        SELECT a.stock_code,a.close_price,a.stock_date
        from stock.stock_history_date_price a
        join (
        SELECT max(stock_date) stock_date,stock_code
        from stock.stock_history_date_price
        where date_format(stock_date,'%Y-%m-%d') >= '{first_trade_week}'
        and date_format(stock_date,'%Y-%m-%d') <= '{last_trade_week}'
        GROUP BY stock_code
        ) b on a.stock_date = b.stock_date
        and a.stock_code = b.stock_code)
        select a.stock_code,SUBSTRING_INDEX(a.stock_code,'.',-1) stock_id ,
            c.stock_date,b.open_price,a.high_price,a.low_price,
            c.close_price,a.trading_volume,a.trading_amount,
            3 as adjust_flag,turn,round(((c.close_price - last_close_price)/last_close_price) * 100,4) increase_and_decrease
        from
        (SELECT stock_code,sum(trading_volume) trading_volume ,
        sum(trading_amount)trading_amount,min(low_price)low_price,
        max(high_price) high_price,sum(turn) turn
        from stock.stock_history_date_price
        where date_format(stock_date,'%Y-%m-%d') >= '{first_trade_week}'
        and date_format(stock_date,'%Y-%m-%d') <= '{last_trade_week}'
        GROUP BY stock_code) a join
        week_open_price b on a.stock_code = b.stock_code
        join
        week_close_price c on a.stock_code = c.stock_code;
    '''
    engine = my.get_mysql_connection()
    result = my.execute_read_query(engine, calculate_sql)
    df = pd.DataFrame(result)

    if len(df) == 0:
        return
    df['stock_date'] = df['stock_date'].map(tu.find_last_trading_day_of_week)
    placeholders = gs.get_stock_code(df)

    cnt = my.batch_insert_or_update(engine, df, 'stock_history_week_price', 'stock_code', 'stock_date')
    if cnt > 0:
        now_date = tu.get_last_some_time(0)
        sql = f'''
        update update_stock_record set update_stock_week = '{now_date}' where stock_code in ({placeholders});
        '''
        my.execute_query(engine, sql)


# 识别MACD底背离
def detect_macd_divergence(df, price_col='close_price', macd_col='macd',
                           window=20, min_interval=14, trend_length=30):
    # 数据校验
    if not isinstance(df, pd.DataFrame):
        raise TypeError("输入必须是 pandas.DataFrame")
    if df.empty:
        return pd.DataFrame()
    if price_col not in df.columns or macd_col not in df.columns:
        raise ValueError("数据必须包含close_price和macd列")

    # ==== 日期预处理 ====
    # 确保存在stock_date列且为正确日期格式
    if 'stock_date' not in df.columns:
        raise ValueError("数据必须包含stock_date列")

    # 转换日期列为datetime类型（处理多种格式）
    try:
        df['stock_date'] = pd.to_datetime(
            df['stock_date'],
            format='%Y%m%d',  # 处理形如20230719的数值格式
            errors='coerce'
        )
    except:
        df['stock_date'] = pd.to_datetime(
            df['stock_date'],
            format='mixed',  # 处理字符串和datetime混合格式
            errors='coerce'
        )

    # 过滤无效日期
    invalid_dates = df[df['stock_date'].isnull()]
    if not invalid_dates.empty:
        print(f"警告：发现{len(invalid_dates)}条无效日期记录，已自动过滤")
        df = df.dropna(subset=['stock_date'])

    df['close_price'] = df['close_price'].astype(float)
    df['macd'] = df['macd'].astype(float)

    # 日期索引处理
    try:
        # 设置日期索引并按时间排序
        df = df.set_index('stock_date').sort_index()
    except:
        pass  # 已经是日期索引则跳过

    data = df.copy()

    # 改进的极值点识别
    def find_valid_lows(series, window):
        lows = []
        for i in argrelextrema(series.values, np.less, order=window)[0]:
            if i < window or i > len(series) - window:
                continue
            # 要求比前window日最低点低至少1%
            if series.iloc[i] < series.iloc[i - window:i].min() * 0.99:
                lows.append(i)
        return np.array(lows)

    price_lows = find_valid_lows(data[price_col], window)
    macd_lows = find_valid_lows(data[macd_col], window)

    # 趋势计算
    data['trend'] = data[price_col].rolling(trend_length).apply(
        lambda x: (x[0] - x[-1]) / x[0] if x[0] != 0 else 0, raw=True)

    min_interval = max(min_interval, window // 2)  # 动态间隔
    new_signals = []
    # 信号检测
    for i in range(1, len(price_lows)):
        prev_idx = price_lows[i - 1]
        curr_idx = price_lows[i]

        # 时间间隔过滤
        if (curr_idx - prev_idx) < min_interval:
            continue

        # 价格条件
        if data[price_col].iloc[curr_idx] >= data[price_col].iloc[prev_idx]:
            continue

        # MACD过滤
        macd_mask = (macd_lows >= prev_idx) & (macd_lows <= curr_idx)
        if not macd_mask.any():
            continue
        macd_low_idx = macd_lows[macd_mask][-1]

        if data[macd_col].iloc[macd_low_idx] <= data[macd_col].iloc[prev_idx]:
            continue

        # 趋势确认
        if data['trend'].iloc[curr_idx] < 0.08:  # 下跌不足8%不视为有效趋势
            continue

        # 收集信号到列表
        new_signals.append({
            'signal_date': data.index[curr_idx],
            'price_low1': data[price_col].iloc[prev_idx],
            'price_low2': data[price_col].iloc[curr_idx],
            'macd_low1': data[macd_col].iloc[prev_idx],
            'macd_low2': data[macd_col].iloc[macd_low_idx],
            'trend_strength': data['trend'].iloc[curr_idx]
        })

    return pd.DataFrame(new_signals)


def calculate_today_stock_boll():
    # 获取数据库连接
    engine = my.get_mysql_connection()
    stock_list = gs.get_stock_list_for_update_df()
    update_table = 'stock_date_boll'
    for record in stock_list.values:
        stock_code = record[0]
        stock_name = record[1]
        try:
            # 使用参数化查询防止 SQL 注入
            sql = f'''
            select a.stock_code,b.stock_name,a.stock_date,a.close_price,b.stock_ma20,rn from
            (SELECT * from
            (SELECT *,ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY stock_date asc) rn
            from stock_history_date_price where stock_code = (
            select stock_code from stock.stock_basic where stock_code = '{stock_code}'
            ) and tradestatus = 1)a) a
            join date_stock_moving_average_table b on
            a.stock_code = b.stock_code
            and a.stock_date = b.stock_date;
            '''
            result = my.execute_read_query(engine, sql)
            # 将结果转换为 DataFrame
            df = pd.DataFrame(result)
            length = len(df)
            # 数据校验
            if df.empty:
                print(f"股票 {stock_code} 无数据，跳过处理。")
                continue
            if len(df) < 20:
                print(f"股票 {stock_code} 数据不足20天，无法计算布林线。")
                continue
            print(f'开始计算<{stock_name}>布林线...')
            # 向量化计算布林线指标
            df['boll_twenty'] = df['stock_ma20'].astype(float)  # 中轨直接使用MA20
            df['std_dev'] = df['close_price'].astype(float).rolling(window=20, min_periods=20).std(ddof=1)
            df['upper_rail'] = df['boll_twenty'] + 2 * df['std_dev']
            df['lower_rail'] = df['boll_twenty'] - 2 * df['std_dev']

            # 清理无效数据（前19天无法计算）
            df.dropna(subset=['upper_rail', 'lower_rail'], inplace=True)

            # 准备写入数据（明确创建副本）
            data_df = df[['stock_code', 'stock_name', 'stock_date', 'boll_twenty', 'upper_rail', 'lower_rail']].copy()
            data_df = data_df.replace({np.nan: None})

            # # 按日期降序排列结果（便于查看最新数据）
            # data_df = data_df.sort_values('stock_date', ascending=False)
            # 批量写入数据库
            if not data_df.empty:
                my.batch_insert_or_update(engine, data_df, update_table, 'stock_code', 'stock_date')
                print(f"成功更新<{stock_name}>的布林线数据，共{len(data_df)}条记录。")
            else:
                print(f"无有效布林线数据可更新。")
        except Exception as e:
            print(f"处理股票 {stock_code} 时发生错误：{str(e)}")
            raise e


def calculate_today_stock_cci():
    # 获取数据库连接
    engine = my.get_mysql_connection()
    stock_list = gs.get_stock_list_for_update_df()
    update_table = 'stock_date_cci'
    for record in stock_list.values:
        stock_code = record[0]
        stock_name = record[1]
        try:
            # 使用参数化查询防止 SQL 注入
            sql = f'''
            select a.stock_code,b.stock_name,a.stock_date,a.close_price,b.stock_ma20,rn,
                   a.open_price,a.high_price,a.low_price,
                   ((a.high_price + a.low_price + a.close_price)/3) as tp
             from
            (SELECT * from
            (SELECT *,ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY stock_date asc) rn
            from stock_history_date_price where stock_code = (
            select stock_code from stock.stock_basic where stock_code = '{stock_code}'
            ) and tradestatus = 1)a) a
            join date_stock_moving_average_table b on
            a.stock_code = b.stock_code
            and a.stock_date = b.stock_date;
            '''
            result = my.execute_read_query(engine, sql)
            # 将结果转换为 DataFrame
            df = pd.DataFrame(result)
            length = len(df)
            if df.empty:
                print(f"股票 {stock_code} 无数据，跳过处理。")
                continue

            print(f'开始计算<{stock_name}>的CCI指标（全量历史数据）...')

            # 计算14日滚动窗口的SMA和MAD
            df['sma14'] = df['tp'].astype(float).rolling(window=14, min_periods=14).mean()
            df['mad'] = df['tp'].astype(float).rolling(window=14, min_periods=14).apply(
                lambda x: np.mean(np.abs(x - x.mean())), raw=True
            )

            # 计算CCI并清理无效数据
            df['cci'] = (df['tp'].astype(float) - df['sma14'].astype(float)) / (0.015 * df['mad'])
            df.dropna(subset=['cci'], inplace=True)  # 删除前13天无法计算的行

            # # 按日期降序排列结果（便于查看最新数据）
            # df = df.sort_values('stock_date', ascending=False)

            # 准备写入数据库
            data_df = df[['stock_code', 'stock_name', 'stock_date', 'tp', 'mad', 'cci']]
            data_df = data_df.replace({np.nan: None})
            if len(data_df) > 0:
                my.batch_insert_or_update(engine, data_df, update_table, 'stock_code', 'stock_date')
                print(f'计算<{stock_name}> cci成功！')
        except Exception as e:
            raise e


def calculate_stock_rsi():
    engine = my.get_mysql_connection()
    stock_list = gs.get_stock_list_for_update_df()
    # stock_list = stock_list[stock_list['stock_code'] == 'sh.600000']
    update_table = 'stock_date_rsi'
    for record in stock_list.values:
        stock_code = record[0]
        stock_name = record[1]
        sql = f'''
        select a.stock_code,b.stock_name,a.stock_date,a.close_price from 
        (select stock_code,stock_date,close_price from stock.stock_history_date_price where stock_code = '{stock_code}' 
        and tradestatus = 1 ) a join (select stock_code,stock_name from stock.stock_basic where stock_code = '{stock_code}') b 
        on a.stock_code = b.stock_code;
        '''
        result = my.execute_read_query(engine, sql)
        df = pd.DataFrame(result)
        # 按股票分组计算
        print(f'开始计算<{stock_name}>rsi值')
        result_dfs = []
        for stock, group in df.groupby('stock_code'):
            # 排序确保日期顺序
            group = group.sort_values('stock_date')

            # 计算各周期RSI
            for window in [6, 12, 24]:
                group[f'rsi_{window}'] = dynamic_window(group, window)

            result_dfs.append(group)

        # 合并结果
        final_df = pd.concat(result_dfs)
        final_df = final_df[final_df['rsi_6'].notna()]
        # 注意：仅针对RSI列操作，保留其他字段原始值
        rsi_columns = ['rsi_6', 'rsi_12', 'rsi_24']
        final_df[rsi_columns] = final_df[rsi_columns].replace({np.nan: None})
        if len(final_df) > 0:
            my.batch_insert_or_update(engine, final_df, update_table, 'stock_code', 'stock_date')
            print(f'<{stock_name}>rsi值完成计算')


def dynamic_window(data, window):
    # 计算价格变化
    delta = data['close_price'].astype(float).diff()

    # 分离上涨和下跌
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    # 计算初始平均涨幅/跌幅（SMA）
    avg_gain = gain.rolling(window).mean()
    avg_loss = loss.rolling(window).mean()

    # 计算后续EMA
    for i in range(window, len(data)):
        avg_gain.iloc[i] = (avg_gain.iloc[i - 1] * (window - 1) + gain.iloc[i]) / window
        avg_loss.iloc[i] = (avg_loss.iloc[i - 1] * (window - 1) + loss.iloc[i]) / window

    # 计算RS和RSI
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi



if __name__ == '__main__':
    # # 参数配置
    # params = {
    #     'window': 30,  # 增大窗口减少噪声
    #     'min_interval': 21,  # 约1个月间隔
    #     'trend_length': 60  # 2个月趋势观察
    # }
    #
    # df = gs.get_stock_price_record_and_macd('sh.600392', 'd')
    # signals = detect_macd_divergence(df, **params)
    #
    # # 格式化输出
    # if not signals.empty:
    #     print(f"发现 {len(signals)} 个有效底背离信号（参数：{params}）")
    #     print("=" * 60)
    #
    #     for idx, row in signals.iterrows():
    #         print(f"""信号 #{idx + 1}
    # 发生时间：{row['signal_date'].strftime('%Y-%m-%d')}
    # 价格变化：{row['price_low1']:.2f} → {row['price_low2']:.2f} (跌幅 {((row['price_low1'] - row['price_low2']) / row['price_low1'] * 100):.1f}%)
    # MACD变化：{row['macd_low1']:.3f} → {row['macd_low2']:.3f} (升幅 {((row['macd_low2'] - row['macd_low1']) / abs(row['macd_low1']) * 100 if row['macd_low1'] != 0 else 0):.1f}%)
    # 趋势强度：过去{params['trend_length']}日累计下跌 {row['trend_strength'] * 100:.1f}%
    # {'-' * 60}""")
    # else:
    #     print("未检测到符合条件的底背离信号")
    calculate_stock_rsi()
