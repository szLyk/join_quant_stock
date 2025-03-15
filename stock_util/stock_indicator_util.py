from mysql.connector import Error

import util.mysql_util as my
import pandas as pd
from util.time_util import find_last_trading_day_of_week, find_last_trading_day_of_month
import util.get_stock as gs
from decimal import Decimal, getcontext
import util.file_util as fu


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


if __name__ == '__main__':
    calculate_stock_macd('d')
