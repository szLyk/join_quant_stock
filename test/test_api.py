import util.mysql_util as my
import util.get_stock as gs
import pandas as pd

from stock_util.stock_indicator_util import calculate_stock_ma, calculate_stock_month_price, calculate_stock_macd, \
    calculate_stock_week_price, calculate_stock_cci, calculate_stock_rsi, \
    calculate_stock_boll, calculate_stock_obv
from util.time_util import get_last_some_time, random_pause


def update_stock_ma():
    sql = f'''
    select stock_code from (select stock_code,min(stock_date) min_stock_date from 
    stock_history_date_price
    group by stock_code ) a 
    where min_stock_date < '2000-01-04';
    '''
    stock_list = my.execute_read_query(my.get_mysql_connection(),sql)
    df = pd.DataFrame(stock_list)
    engine = my.get_mysql_connection()
    for record in df.values:
        select_sql = f'''
        -- 插入或更新 target_table 表
        INSERT INTO date_stock_moving_average_table (stock_code, stock_date, stock_ma9)
        SELECT 
            stock_code,
            stock_date,
            stock_ma9
        FROM (
            WITH RankedPrices AS (
                SELECT 
                    stock_code,
                    stock_name,
                    stock_date,
                    close_price,
                    COUNT(*) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) as count_9d
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
                                         and stock_date <= '2025-03-12') a
                     JOIN
                        (SELECT stock_code, stock_name FROM update_stock_record where stock_code = '{record[0]}') b 
                     ON a.stock_code = b.stock_code) a 
            )
            SELECT 
                stock_code,
                stock_date,
                CASE WHEN count_9d >= 9 THEN AVG(close_price) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) END AS stock_ma9
            FROM 
                RankedPrices
        ) AS subquery
        ON DUPLICATE KEY UPDATE
            stock_ma9 = VALUES(stock_ma9);
        '''
        print(f'<{record[0]}> 更新完毕...')
        my.execute_query(engine, select_sql)


def update_stock_trade_date():
    engine = my.get_mysql_connection()
    sql = f'''
    select stock_code from (select stock_code,min(stock_date) min_stock_date from 
    stock_history_date_price
    group by stock_code ) a 
    where min_stock_date = '2000-01-04';
    '''
    result = my.execute_read_query(engine, sql)
    df = pd.DataFrame(result)
    for i in df.values:
        df = gs.get_some_stock_data(i[0], '1990-01-01', '2000-01-04', "d")
        gs.insert_batch_into_stock_price_record('d', df)
        random_pause(2)


if __name__ == '__main__':
    # gs.update_all_stock_history_date_week_month_price('w')
    # calculate_stock_week_price()
    # calculate_stock_month_price()
    # calculate_stock_ma('w')
    # calculate_stock_macd('w')
    # calculate_stock_macd('m')

    # gs.update_all_stock_today_price('d')
    # calculate_stock_week_price()
    # calculate_stock_month_price()
    # calculate_stock_ma('d')
    # calculate_stock_macd('d')
    # calculate_today_stock_boll()
    # calculate_today_stock_cci()
    calculate_stock_rsi('d')
    calculate_stock_rsi('w')
    calculate_stock_rsi('m')
    calculate_stock_cci('d')
    calculate_stock_cci('w')
    calculate_stock_cci('m')
    # calculate_stock_obv('d')
    # calculate_stock_obv('w')
    # calculate_stock_obv('m')


