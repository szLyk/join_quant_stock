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


# 获取要计算的股票的MACD
def calculate_stock_macd(frequency):
    start_time = time.time()
    batch_size = 100  # 进一步增大批次量
    engine = my.get_mysql_connection()

    # 频率配置字典
    freq_config = {
        'd': ('stock_history_date_price', 'date_stock_macd', 'update_stock_date_macd', 60, '日'),
        'w': ('stock_history_week_price', 'week_stock_macd', 'update_stock_week_macd', 100, '周'),
        'm': ('stock_history_month_price', 'month_stock_macd', 'update_stock_month_macd', 1500, '月')
    }
    select_table, insert_table, update_col, days, fre = freq_config[frequency[0]]
    record_table = 'update_stock_record'
    # stock_list = gs.get_redis_stock_list(update_col)

    # if len(stock_list) == 0:
    #     stock_list = gs.get_stock_list_for_update_df()[['stock_code', 'stock_name']]
    #     stock_list = stock_list['stock_code'] + ':' + stock_list['stock_name']
    #     gs.set_redis_stock_list(update_col, stock_list)

    sql = f'''
    select stock_code from stock.stock_basic
    where stock_code = 'sh.600000';
    '''
    result = my.execute_read_query(engine, sql)
    print(result)
    stock_list = pd.DataFrame(result)

    # 预定义EMA参数
    alpha_12 = 2 / 13
    alpha_26 = 2 / 27
    alpha_9 = 2 / 10

    # 获取待处理股票列表
    # stock_list = gs.split_stock_name(stock_list)
    for i in range(0, len(stock_list), batch_size):
        batch_df = stock_list.iloc[i:i + batch_size]
        batch_codes = batch_df['stock_code'].unique()
        code_str = ", ".join([f"'{code}'" for code in batch_codes])
        print(f'开始计算{fre}MACD {code_str}')
        # 一次性获取所有股票的更新状态和最新MACD记录
        status_sql = f"""        
        select stock_code, stock_date, ema_12, ema_26, dea, diff, macd from 
        (select a.stock_code, stock_date, ema_12, ema_26, dea, diff, macd,
            row_number() over (partition by a.stock_code order by a.stock_date) rn       
        from
        (SELECT stock_code, stock_date, ema_12, ema_26, dea, diff, macd
                FROM stock.{insert_table} ) a
        join (SELECT stock_code, update_stock_date_macd
                    FROM stock.update_stock_record
                    WHERE stock_code IN ({code_str})) b
        on a.stock_code = b.stock_code
        and a.stock_date >= DATE_SUB( b.update_stock_date_macd, INTERVAL {days} DAY)) a 
        where rn = 1
        """
        print(status_sql)
        result = my.execute_read_query(engine, status_sql)
        status_df = pd.DataFrame(result,
                                 columns=['stock_code', 'stock_name', 'ema_12', 'ema_26', 'dea', 'diff', 'macd'])
        status_dict = status_df.set_index('stock_code').to_dict('index')

        # 获取需要处理的价格数据（智能过滤）
        price_sql = f"""
        SELECT p.stock_code,stock_name, p.stock_date, p.close_price
        FROM stock.{select_table} p
        JOIN (
            SELECT stock_code,stock_name,
                   DATE_SUB({update_col},INTERVAL {days} DAY) as start_date
            FROM stock.update_stock_record
            WHERE stock_code IN ({code_str})
        ) s ON p.stock_code = s.stock_code 
           AND p.stock_date >= s.start_date
        ORDER BY p.stock_code, p.stock_date
        """

        print(price_sql)

        price_df = my.execute_read_query(engine, price_sql)
        price_df = pd.DataFrame(price_df, columns=['stock_code', 'stock_name', 'stock_date', 'close_price'])
        if price_df.empty:
            continue

        # 向量化计算函数
        def compute_macd(group):
            code = group.name
            group = group.sort_values(by='stock_date', ascending=True).reset_index(drop=True)
            status = status_dict.get(code, {})
            closes = group['close_price'].astype(float).values
            dates = group['stock_date'].values
            name = group['stock_name'].values

            # 初始化数组
            n = len(closes)
            ema12 = np.empty(n)
            ema26 = np.empty(n)
            diff = np.empty(n)
            dea = np.empty(n)
            macd = np.empty(n)

            # 判断初始化状态
            if not status:
                # 全新初始化
                ema12[0] = ema26[0] = group['close_price'].iloc[0]
                diff[0] = dea[0] = 0.0
            else:
                # 增量计算
                ema12[0] = status['ema_12']
                ema26[0] = status['ema_26']
                diff[0] = status['diff']
                dea[0] = status['dea']

            macd[0] = status.get('macd')

            # 向量化迭代计算
            for i in range(1, n):
                ema12[i] = alpha_12 * closes[i] + (1 - alpha_12) * ema12[i - 1]
                ema26[i] = alpha_26 * closes[i] + (1 - alpha_26) * ema26[i - 1]
                diff[i] = ema12[i] - ema26[i]
                dea[i] = alpha_9 * diff[i] + (1 - alpha_9) * dea[i - 1]
                macd[i] = (diff[i] - dea[i]) * 2

            return pd.DataFrame({
                'stock_code': code,
                'stock_name': name,
                'stock_date': dates,
                'close_price': closes,
                'ema_12': ema12,
                'ema_26': ema26,
                'diff': diff,
                'dea': dea,
                'macd': macd
            })

        # 分组并行计算
        macd_df = price_df.groupby('stock_code', group_keys=False).apply(compute_macd).reset_index(level=0, drop=True)
        nan_stock_code = macd_df[macd_df['macd'].isnull()]['stock_code'].unique()
        if len(nan_stock_code) > 0:
            macd_df = macd_df[~macd_df['stock_code'].isin(nan_stock_code)]

        print(macd_df)
        #
        # # 批量更新
        # if not macd_df.empty:
        #     # 更新MACD数据
        #     cnt = my.batch_insert_or_update(engine, macd_df, insert_table, 'stock_code', 'stock_date')
        #     if cnt > 0:
        #         # 直接更新状态表
        #         max_dates = macd_df.groupby('stock_code')['stock_date'].max().reset_index()
        #         max_dates.rename(columns={'stock_date': update_col}, inplace=True)
        #         cnt = my.insert_or_update(engine, max_dates, record_table, 'stock_code')
        #         if cnt > 0:
        #             stock_names = batch_df['stock_code'] + ":" + batch_df['stock_name']
        #             for code in stock_names:
        #                 gs.remove_redis_update_stock_code(update_col, code)
        #         print(f'结束计算{fre}MACD {code_str}')
    # 记录结束时间
    end_time = time.time()
    # 计算执行时间
    execution_time = end_time - start_time
    print(f"程序执行时间: {execution_time:.6f} 秒")

if __name__ == '__main__':
    calculate_stock_macd('m')
