import util.mysql_util as my
import util.get_stock as gs
import pandas as pd

from stock_util.stock_indicator_util import calculate_stock_ma, calculate_stock_macd, calculate_stock_week_price, \
    calculate_stock_month_price, calculate_stock_rsi, calculate_stock_cci, \
    calculate_stock_boll, calculate_stock_obv
from util.time_util import get_last_some_time, random_pause, find_last_trading_day_of_month, \
    find_last_trading_day_of_week


def update_stock_ma():
    sql = f'''
    select stock_code from (select stock_code,min(stock_date) min_stock_date from 
    stock_history_date_price
    group by stock_code ) a 
    where min_stock_date < '2000-01-04';
    '''
    stock_list = my.execute_read_query(my.get_mysql_connection(), sql)
    df = pd.DataFrame(stock_list)
    engine = my.get_mysql_connection()
    for record in df.values:
        print(f'<{record[0]}> 开始计算....')
        select_sql = f'''
        -- 插入或更新 target_table 表
        INSERT INTO stock.date_stock_moving_average_table (stock_code, stock_name, stock_date, close_price, stock_ma3, stock_ma5, stock_ma6, stock_ma7, stock_ma9, stock_ma10, stock_ma12, stock_ma20, stock_ma24, stock_ma26, stock_ma30, stock_ma60, stock_ma70, stock_ma125, stock_ma250)
        SELECT
            stock_code, stock_name, stock_date, close_price, stock_ma3, stock_ma5, stock_ma6, stock_ma7, stock_ma9, stock_ma10, stock_ma12, stock_ma20, stock_ma24, stock_ma26, stock_ma30, stock_ma60, stock_ma70, stock_ma125, stock_ma250
        FROM (
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
                         FROM stock_history_date_price
                         WHERE tradestatus = 1 AND stock_date >= DATE_SUB('1990-01-01', INTERVAL 12 MONTH)
                                         and stock_date <= '2001-01-01') a
                     JOIN
                        (SELECT stock_code, stock_name FROM update_stock_record where stock_code = '{record[0]}') b
                     ON a.stock_code = b.stock_code) a
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
                RankedPrices
        ) AS subquery
        ON DUPLICATE KEY UPDATE
            stock_name = VALUES(stock_name),
            close_price = VALUES(close_price), 
            stock_ma3 = VALUES(stock_ma3), 
            stock_ma5 = VALUES(stock_ma5), 
            stock_ma6 = VALUES(stock_ma6), 
            stock_ma7 = VALUES(stock_ma7), 
            stock_ma9 = VALUES(stock_ma9), 
            stock_ma10 = VALUES(stock_ma10), 
            stock_ma12 = VALUES(stock_ma12), 
            stock_ma20 = VALUES(stock_ma20), 
            stock_ma24 = VALUES(stock_ma24), 
            stock_ma26 = VALUES(stock_ma26), 
            stock_ma30 = VALUES(stock_ma30), 
            stock_ma60 = VALUES(stock_ma60), 
            stock_ma70 = VALUES(stock_ma70), 
            stock_ma125 = VALUES(stock_ma125), 
            stock_ma250 = VALUES(stock_ma250);
        '''
        print(f'<{record[0]}> 更新完毕...')
        my.execute_query(engine, select_sql)


def update_stock_week_or_month_date():
    sql = f'''
    SELECT stock_code,stock_date from stock.date_stock_moving_average_table 
    where stock_code in (
    select stock_code from (select stock_code,min(stock_date) min_stock_date from 
    stock_history_date_price
    group by stock_code ) a 
    where min_stock_date < '2000-01-04') 
    and stock_date  < '2000-01-04';
    '''
    engine = my.get_mysql_connection()
    stock_list = my.execute_read_query(my.get_mysql_connection(), sql)
    df = pd.DataFrame(stock_list)
    # 假设 find_last_trading_day_of_week 函数已定义
    for index, row in df.iterrows():
        week_date = find_last_trading_day_of_week(row['stock_date'])
        month_date = find_last_trading_day_of_month(row['stock_date'])  # 注意这里是月度日期计算，可能需要不同的函数
        df.loc[index, 'stock_week_date'] = week_date
        df.loc[index, 'stock_month_date'] = month_date

        update_sql = f'''
        INSERT INTO stock.date_stock_moving_average_table (stock_code, stock_date, stock_week_date,stock_month_date)
        VALUES ('{df.loc[index, 'stock_code']}','{df.loc[index, 'stock_date']}','{df.loc[index, 'stock_week_date']}','{df.loc[index, 'stock_month_date']}')
        ON DUPLICATE KEY UPDATE stock_week_date = VALUES(stock_week_date),
        stock_month_date = VALUES(stock_month_date);
        '''
        stock_code = df.loc[index, 'stock_code']
        my.execute_query(engine,update_sql)
        print(f'更新成功! <{stock_code}>')


if __name__ == '__main__':
    gs.update_all_stock_today_price('d')
    calculate_stock_week_price()
    calculate_stock_month_price()
    calculate_stock_ma('d')
    calculate_stock_ma('w')
    calculate_stock_ma('m')
    calculate_stock_macd('d')
    calculate_stock_macd('w')
    calculate_stock_macd('m')
    calculate_stock_boll('d')
    calculate_stock_boll('w')
    calculate_stock_boll('m')
    calculate_stock_obv('d')
    calculate_stock_obv('w')
    calculate_stock_obv('m')

