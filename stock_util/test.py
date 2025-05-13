import time
import datetime
import baostock as bs
import pandas as pd
import numpy as np
import util.mysql_util as my
import util.get_stock as gs
import util.time_util as tu
import util.redis_util as ru
import util.mysql_util as my


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


if __name__ == '__main__':
    gs.update_all_stock_history_date_week_month_price('m')
