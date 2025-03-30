import time

import pandas as pd
import numpy as np
import util.mysql_util as my
import util.get_stock as gs


def calculate_stock_obv(frequency, batch_size=10, window=30):
    start_time = time.time()
    # 频率配置字典
    freq_config = {
        'd': ('stock_history_date_price', 'stock_date_obv', 'update_stock_date_obv', 'AND tradestatus = 1'),
        'w': ('stock_history_week_price', 'stock_week_obv', 'update_stock_week_obv', ''),
        'm': ('stock_history_month_price', 'stock_month_obv', 'update_stock_month_obv', '')
    }
    select_table, insert_table, update_col, trade_status = freq_config[frequency[0]]
    engine = my.get_mysql_connection()
    record_table = 'update_stock_record'
    stock_list = gs.get_redis_stock_list(update_col)
    if len(stock_list) == 0:
        stock_list = gs.get_stock_list_for_update_df()[['stock_code', 'stock_name']]
        stock_list = stock_list['stock_code'] + ':' + stock_list['stock_name']
        gs.set_redis_stock_list(update_col, stock_list)
    stock_list = gs.split_stock_name(stock_list)
    for i in range(0, len(stock_list), batch_size):
        batch_df = stock_list.iloc[i:i + batch_size]
        batch_codes = batch_df['stock_code'].unique()
        batch_names = batch_df['stock_name'].unique()
        code_str = ", ".join([f"'{code}'" for code in batch_codes])
        sql = f'''
            SELECT a.stock_code,b.stock_name,a.stock_date,a.close_price,a.open_price,a.trading_volume
            FROM (
                SELECT stock_code,stock_date,close_price,open_price,trading_volume
                FROM stock.{select_table} 
                WHERE stock_code in ({code_str})
                {trade_status}
            ) a 
            JOIN (
                SELECT stock_code,stock_name 
                FROM stock.stock_basic 
                WHERE stock_code in ({code_str})
                and stock_type = 1 and stock_status = 1
            ) b 
            ON a.stock_code = b.stock_code
        '''
        df = pd.DataFrame(my.execute_read_query(engine, sql))
        # 类型转换
        df[['open_price', 'close_price', 'trading_volume']] = df[
            ['open_price', 'close_price', 'trading_volume']].astype(
            float)
        # 类型转换与数据清洗
        df['open_price'] = pd.to_numeric(df['open_price'], errors='coerce')
        df['close_price'] = pd.to_numeric(df['close_price'], errors='coerce')
        df['trading_volume'] = pd.to_numeric(df['trading_volume'], errors='coerce')
        df = df.dropna(subset=['open_price', 'close_price', 'trading_volume'])
        max_dates = []

        def calculate_obv_group(group):
            """OBV计算核心函数（按股票分组处理）"""
            # 确保时间序列正确排序
            group = group.sort_values('stock_date')
            max_date = group['stock_date'].max()
            stock_name = group['stock_name'].iloc[0]
            # 计算价格变化量
            first_day_mask = group.index == group.index.min()  # 更精确的首日判断
            delta = np.where(
                first_day_mask,
                group['close_price'] - group['open_price'],  # 首日使用开盘收盘价差
                group['close_price'].diff()  # 后续使用收盘价差分
            )

            # 计算成交量方向
            sign = np.select(
                [delta > 0, delta < 0],
                [1, -1],
                default=0
            )

            # 累计OBV
            group['obv'] = (sign * group['trading_volume']).cumsum()
            max_dates.append([stock_name, max_date])
            return group

        def calculate_maobv_group(group, window=30):
            """MAOBV计算函数（独立分组处理）"""
            group['30ma_obv'] = group['obv'].rolling(
                window=window,
                min_periods=window  # 严格模式：必须满30日
            ).mean()
            return group

        # 按股票代码和日期排序（确保时间序列正确）
        df = df.sort_values(by=['stock_code', 'stock_date'])
        # 分组计算OBV
        df = df.groupby('stock_code', group_keys=False).apply(calculate_obv_group)
        # 分组计算MAOBV
        df = df.groupby('stock_code', group_keys=False).apply(
            lambda g: calculate_maobv_group(g, window)
        )
        final_df = df[['stock_code', 'stock_name', 'stock_date', 'trading_volume', 'obv', '30ma_obv']].replace({np.nan: None})
        if len(final_df) > 0:
            cnt = my.batch_insert_or_update(engine, final_df, insert_table, 'stock_code', 'stock_date', batch_size=1500)
            if cnt > 0:
                stock_names = batch_df['stock_code'] + ":" + batch_df['stock_name']
                for code in stock_names:
                    gs.remove_redis_update_stock_code(update_col, code)
                update_record = pd.DataFrame(max_dates, columns=['stock_name', f'{update_col}'])
                my.insert_or_update(engine, update_record, record_table, 'stock_code')
            print(f'{batch_names}RSI值计算完成')
    # 记录结束时间
    end_time = time.time()
    # 计算执行时间
    execution_time = end_time - start_time
    print(f"程序执行时间: {execution_time:.6f} 秒")


# 执行计算
calculate_obv_with_ma('d')

