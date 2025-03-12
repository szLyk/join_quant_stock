import util.mysql_util as my
import pandas as pd
from util.time_util import find_last_trading_day_of_week, find_last_trading_day_of_month


# 获取要计算的股票数据
def calculate_stock_ma(frequency):
    table = 'stock_history_date_price'
    column = 'update_stock_date_ma'
    if frequency == 'm':
        table = 'stock_history_month_price'
        column = 'update_stock_month_ma'
    elif frequency == 'w':
        table = 'stock_history_week_price'
        column = 'update_stock_week_ma'

    get_stock_start_date = f'''
    SELECT a.stock_code,if(a.max_stock_date > b.{column},b.{column},a.max_stock_date) update_date from
    (SELECT a.stock_code,max(a.stock_date) max_stock_date from stock.stock_history_date_price a GROUP BY a.stock_code) a
    join (SELECT b.stock_code,b.{column} from stock.update_stock_record b) b on a.stock_code = b.stock_code
   '''

    conn = my.get_mysql_connection()
    stock_start_date_result = my.execute_read_query(conn, get_stock_start_date)
    stock_start_date_df = pd.DataFrame(stock_start_date_result)

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
                        COUNT(*) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as count_7d,
                        COUNT(*) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as count_10d,
                        COUNT(*) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) as count_12d,
                        COUNT(*) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as count_20d,
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
                             where tradestatus = 1 and stock_date >= DATE_SUB('{update_date}', INTERVAL 12 MONTH)) a
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
                            CASE WHEN count_7d >= 7 THEN AVG(close_price) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) END AS stock_ma7,
                            CASE WHEN count_10d >= 10 THEN AVG(close_price) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) END AS stock_ma10,
                            CASE WHEN count_12d >= 12 THEN AVG(close_price) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) END AS stock_ma12,
                            CASE WHEN count_20d >= 20 THEN AVG(close_price) OVER (PARTITION BY stock_code ORDER BY stock_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) END AS stock_ma20,
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
        ma_df['stock_week_date'] = ma_df['stock_date'].map(find_last_trading_day_of_week)
        ma_df['stock_month_date'] = ma_df['stock_date'].map(find_last_trading_day_of_month)
        print(f'计算<{stock_code}>均线...')
        my.insert_or_update(conn, ma_df, 'stock_moving_average_table', 'stock_code', 'stock_date')

        get_max_update_date_sql = f'''
        select max(stock_date) max_stock_date from {table} where stock_code = '{stock_code}'
        '''
        max_update = my.execute_read_query(conn, get_max_update_date_sql)

        update_sql = f'''
        update stock.update_stock_record set update_stock_date_ma = '{max_update[0][0]}' where  stock_code = '{stock_code}'
        '''
        print(f'<{stock_code}>均线计算完毕...')
        my.execute_query(conn, update_sql)


if __name__ == '__main__':
    calculate_stock_ma('d')
