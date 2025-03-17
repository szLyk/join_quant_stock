from mysql.connector import Error

import util.mysql_util as my
import pandas as pd
from util.time_util import find_last_trading_day_of_week, find_last_trading_day_of_month, get_last_some_time
import util.get_stock as gs
import util.file_util as fu
import numpy as np
from scipy.signal import argrelextrema
import matplotlib.pyplot as plt


# 获取要计算的股票数据
def calculate_stock_ma(frequency):
    table = 'stock_history_date_price'
    column = 'update_stock_date_ma'
    moving_table = 'date_stock_moving_average_table'
    trade_status = 'and tradestatus = 1'
    if frequency == 'm':
        table = 'stock_history_month_price'
        column = 'update_stock_month_ma'
        moving_table = 'month_stock_moving_average_table'
        trade_status = ''
    elif frequency == 'w':
        table = 'stock_history_week_price'
        column = 'update_stock_week_ma'
        moving_table = 'week_stock_moving_average_table'
        trade_status = ''

    get_stock_start_date = f'''
    SELECT a.stock_code,if(a.max_stock_date > b.{column},b.{column},a.max_stock_date) update_date from
    (SELECT a.stock_code,max(a.stock_date) max_stock_date from stock.{table} a GROUP BY a.stock_code) a
    join (SELECT b.stock_code,b.{column} from stock.update_stock_record b) b on a.stock_code = b.stock_code;
   '''

    conn = my.get_mysql_connection()
    stock_start_date_result = my.execute_read_query(conn, get_stock_start_date)
    stock_start_date_df = pd.DataFrame(stock_start_date_result)

    try:
        for record in stock_start_date_df.values:
            stock_code = record[0]
            update_date = record[1]
            calculate_sql = f'''
                    WITH RankedPrices AS (
                    SELECT 
                        stock_code,
                        stock_name,
                        stock_date,
                        close_price,
                        COUNT(*) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as count_3d,
                        COUNT(*) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as count_5d,
                        COUNT(*) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) as count_6d,
                        COUNT(*) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as count_7d,
                        COUNT(*) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) as count_9d,
                        COUNT(*) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as count_10d,
                        COUNT(*) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) as count_12d,
                        COUNT(*) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as count_20d,
                        COUNT(*) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) as count_24d,
                        COUNT(*) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 25 PRECEDING AND CURRENT ROW) as count_26d,
                        COUNT(*) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as count_30d,
                        COUNT(*) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) as count_60d,
                        COUNT(*) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 69 PRECEDING AND CURRENT ROW) as count_70d,
                        COUNT(*) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 124 PRECEDING AND CURRENT ROW) as count_125d,
                        COUNT(*) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 249 PRECEDING AND CURRENT ROW) as count_250d
                    FROM 
                        (SELECT a.stock_code,
                                stock_name, 
                                stock_date,
                                close_price
                                FROM		
                                (SELECT
                                    stock_code,
                                    stock_date,
                                    close_price
                                 from {table} 
                                 where stock_date >= DATE_SUB('{update_date}', INTERVAL 12 MONTH) {trade_status}) a
                                         join
                                         (SELECT stock_code,stock_name from update_stock_record where stock_code = '{stock_code}') b 
                                         on a.stock_code = b.stock_code) a 
                        )
                    SELECT 
                            stock_code,
                            stock_name,
                            stock_date,
                            close_price,
                            CASE WHEN count_3d >= 3 THEN AVG(close_price) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) END AS stock_ma3,
                            CASE WHEN count_5d >= 5 THEN AVG(close_price) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) END AS stock_ma5,
                            CASE WHEN count_6d >= 6 THEN AVG(close_price) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) END AS stock_ma6,
                            CASE WHEN count_7d >= 7 THEN AVG(close_price) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) END AS stock_ma7,
                            CASE WHEN count_9d >= 9 THEN AVG(close_price) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) END AS stock_ma9,
                            CASE WHEN count_10d >= 10 THEN AVG(close_price) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) END AS stock_ma10,
                            CASE WHEN count_12d >= 12 THEN AVG(close_price) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) END AS stock_ma12,
                            CASE WHEN count_20d >= 20 THEN AVG(close_price) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) END AS stock_ma20,
                            CASE WHEN count_24d >= 24 THEN AVG(close_price) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) END AS stock_ma24,
                            CASE WHEN count_26d >= 26 THEN AVG(close_price) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 25 PRECEDING AND CURRENT ROW) END AS stock_ma26,
                            CASE WHEN count_30d >= 30 THEN AVG(close_price) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) END AS stock_ma30,
                            CASE WHEN count_60d >= 60 THEN AVG(close_price) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) END AS stock_ma60,
                            CASE WHEN count_70d >= 70 THEN AVG(close_price) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 69 PRECEDING AND CURRENT ROW) END AS stock_ma70,
                            CASE WHEN count_125d >= 125 THEN AVG(close_price) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 124 PRECEDING AND CURRENT ROW) END AS stock_ma125,
                            CASE WHEN count_250d >= 250 THEN AVG(close_price) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 249 PRECEDING AND CURRENT ROW) END AS stock_ma250
                    FROM 
                        RankedPrices;
            '''

            ma_result = my.execute_read_query(conn, calculate_sql)
            ma_df = pd.DataFrame(ma_result)
            if len(ma_df) != 0:
                ma_df['stock_week_date'] = ma_df['stock_date'].map(find_last_trading_day_of_week)
                ma_df['stock_month_date'] = ma_df['stock_date'].map(find_last_trading_day_of_month)
                print(f'计算<{stock_code}>均线...')
                my.batch_insert_or_update(conn, ma_df, moving_table, 'stock_code', 'stock_date')

                get_max_update_date_sql = f'''
                select max(stock_date) max_stock_date from {table} where stock_code = '{stock_code}'
                '''
                max_update = my.execute_read_query(conn, get_max_update_date_sql)

                update_sql = f'''
                update stock.update_stock_record set update_stock_date_ma = '{max_update[0][0]}' where  stock_code = '{stock_code}'
                '''
                print(f'<{stock_code}>均线计算完毕...')
                my.execute_query(conn, update_sql)
            else:
                print(f'<{stock_code}> 数据异常请核查！')
                fu.write_to_file(f'{table}问题数据', f'{stock_code}')

    except Error as e:
        print(f"查询执行失败: {e}")


# 获取要计算的股票的MACD
def calculate_stock_macd(frequency):
    batch_size = 5
    engine = my.get_mysql_connection()
    select_table = 'stock_history_date_price'
    insert_table = 'date_stock_macd'
    update_macd_column = 'update_stock_date_macd'
    if frequency == 'w':
        select_table = 'stock_history_week_price'
        insert_table = 'week_stock_macd'
        update_macd_column = 'update_stock_week_macd'
    if frequency == 'm':
        select_table = 'stock_history_month_price'
        insert_table = 'month_stock_macd'
        update_macd_column = 'update_stock_month_macd'

    stock_list = gs.get_stock_list()
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
            JOIN (SELECT stock_name, stock_code FROM stock.stock_industry
            where stock_code in ({result_string})) a
            ON a.stock_code = b.stock_code
			JOIN (SELECT {update_macd_column},stock_code FROM stock.update_stock_record
			where stock_code in ({result_string})) c
            ON c.stock_code = b.stock_code and b.stock_date >= DATE_SUB( c.{update_macd_column},INTERVAL 7 day)
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
    df['stock_date'] = df['stock_date'].map(find_last_trading_day_of_month)
    placeholders = gs.get_stock_code(df)

    cnt = my.batch_insert_or_update(engine, df, 'stock_history_month_price', 'stock_code', 'stock_date')
    if cnt > 0:
        now_date = get_last_some_time(0)
        sql = f'''
        update update_stock_record set update_stock_month = '{now_date}' where stock_code in ({placeholders});
        '''
        my.execute_query(engine, sql)


def detect_macd_divergence(df, price_col='close_price', macd_col='macd',
                           window=20, min_interval=14, trend_length=30):
    # 数据校验
    if not isinstance(df, pd.DataFrame):
        raise TypeError("输入必须是 pandas.DataFrame")
    if df.empty:
        return pd.DataFrame()
    if price_col not in df.columns or macd_col not in df.columns:
        raise ValueError("数据必须包含close_price和macd列")

    df['close_price'] = df['close_price'].astype(float)
    df['macd'] = df['macd'].astype(float)

    # 日期索引处理
    try:
        df = df.set_index(pd.to_datetime(df.index))
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


if __name__ == '__main__':
    # 参数配置
    params = {
        'window': 30,  # 增大窗口减少噪声
        'min_interval': 21,  # 约1个月间隔
        'trend_length': 60  # 2个月趋势观察
    }

    df = gs.get_stock_price_record_and_macd('sh.600343', 'd')
    signals = detect_macd_divergence(df, **params)

    # 格式化输出
    if not signals.empty:
        print(f"发现 {len(signals)} 个有效底背离信号（参数：{params}）")
        print("=" * 60)

        for idx, row in signals.iterrows():
            print(f"""信号 #{idx + 1}
发生时间：{row['signal_date'].strftime('%Y-%m-%d')}
价格变化：{row['price_low1']:.2f} → {row['price_low2']:.2f} (跌幅 {((row['price_low1'] - row['price_low2']) / row['price_low1'] * 100):.1f}%)
MACD变化：{row['macd_low1']:.3f} → {row['macd_low2']:.3f} (升幅 {((row['macd_low2'] - row['macd_low1']) / abs(row['macd_low1']) * 100 if row['macd_low1'] != 0 else 0):.1f}%)
趋势强度：过去{params['trend_length']}日累计下跌 {row['trend_strength'] * 100:.1f}%
{'-' * 60}""")
    else:
        print("未检测到符合条件的底背离信号")