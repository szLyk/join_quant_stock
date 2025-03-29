import time
from mysql.connector import Error
import numpy as np
import util.mysql_util as my
import pandas as pd
import util.time_util as tu
import util.get_stock as gs
from scipy.signal import argrelextrema
from numpy.lib.stride_tricks import sliding_window_view


# 获取要计算的股票数据
def calculate_stock_ma(frequency, if_init=False, batch_size=20):
    start_time = time.time()
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
                                       columns=['stock_code', 'update_date', 'min_stock_date',
                                                'sub_date_diff'])
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

            ma_result = df.groupby('stock_code')[['stock_code', 'stock_name', 'stock_date', 'close_price']].apply(
                compute_ma).reset_index(level=0, drop=True)
            ma_result = ma_result[
                ['stock_code', 'stock_name', 'stock_date', 'close_price'] + [f'stock_ma{w}' for w in ma_windows]]
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
        # 记录结束时间
        end_time = time.time()
        # 计算执行时间
        execution_time = end_time - start_time
        print(f"程序执行时间: {execution_time:.6f} 秒")
    except Error as e:
        print(f"查询执行失败: {e}")


# 获取要计算的股票的MACD
def calculate_stock_macd(frequency):
    start_time = time.time()
    batch_size = 100  # 进一步增大批次量
    engine = my.get_mysql_connection()

    # 频率配置字典
    freq_config = {
        'd': ('stock_history_date_price', 'date_stock_macd', 'update_stock_date_macd', 7),
        'w': ('stock_history_week_price', 'week_stock_macd', 'update_stock_week_macd', 70),
        'm': ('stock_history_month_price', 'month_stock_macd', 'update_stock_month_macd', 700)
    }
    select_table, insert_table, update_col, days = freq_config[frequency[0]]

    # 预定义EMA参数
    alpha_12 = 2 / 13
    alpha_26 = 2 / 27
    alpha_9 = 2 / 10

    # 获取待处理股票列表
    stock_list = gs.get_stock_list_for_update_df()
    for i in range(0, len(stock_list), batch_size):
        batch_codes = stock_list.iloc[i:i + batch_size]['stock_code'].unique()
        code_str = ", ".join([f"'{code}'" for code in batch_codes])

        # 一次性获取所有股票的更新状态和最新MACD记录
        status_sql = f"""
        SELECT stock_code, stock_date, ema_12, ema_26, dea, diff, macd
        FROM stock.{insert_table}
        WHERE (stock_code, stock_date) IN (
            SELECT stock_code, DATE_SUB( update_stock_date_macd, 
                            INTERVAL {days} DAY) 
            FROM stock.update_stock_record
            WHERE stock_code IN ({code_str})
        )
        """

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

        # 批量更新
        if not macd_df.empty:
            # 更新MACD数据
            my.batch_insert_or_update(engine, macd_df, insert_table, 'stock_code', 'stock_date')
            # 直接更新状态表
            max_dates = macd_df.groupby('stock_code')['stock_date'].max().reset_index()
            max_dates.rename(columns={'stock_date': update_col}, inplace=True)
            my.insert_or_update(engine, max_dates, 'update_stock_record', 'stock_code')

            print(f"Processed {len(max_dates)} stocks, last batch: {batch_codes[-5:]}")
    # 记录结束时间
    end_time = time.time()
    # 计算执行时间
    execution_time = end_time - start_time
    print(f"程序执行时间: {execution_time:.6f} 秒")


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


def calculate_stock_boll(frequency, batch_size=10):
    start_time = time.time()
    engine = my.get_mysql_connection()
    stock_list = gs.get_stock_list_for_update_df()

    # 频率配置字典
    freq_config = {
        'd': ('stock_history_date_price', 'stock_date_boll', 'update_stock_date_boll', 'AND tradestatus = 1'),
        'w': ('stock_history_week_price', 'stock_week_boll', 'update_stock_week_boll', ''),
        'm': ('stock_history_month_price', 'stock_month_boll', 'update_stock_month_boll', '')
    }
    select_table, insert_table, update_col, trade_status = freq_config[frequency[0]]
    update_table = 'update_stock_record'
    for i in range(0, len(stock_list), batch_size):
        batch_codes = stock_list.iloc[i:i + batch_size]['stock_code'].unique()
        code_str = ", ".join([f"'{code}'" for code in batch_codes])
        batch_names = stock_list.iloc[i:i + batch_size]['stock_name'].unique()
        print(f'开始计算{batch_names}布尔值')
        # 批量查询SQL（参数化防注入）
        sql = f"""
        SELECT 
            a.stock_code, b.stock_name, a.stock_date, 
            a.close_price, b.stock_ma20
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY stock_code ORDER BY stock_date ASC
            ) rn
            FROM stock.{select_table} 
            WHERE stock_code IN ({code_str}) 
            {trade_status}
        ) a
        JOIN date_stock_moving_average_table b 
            ON a.stock_code = b.stock_code 
            AND a.stock_date = b.stock_date;
        """
        # 执行批量查询
        result = my.execute_read_query(engine, sql)
        all_data = pd.DataFrame(result)
        if all_data.empty:
            print("无有效数据")
            return
        max_dates = []

        def vectorized_bollinger_bands(close_prices, window=20, num_std=2):
            """向量化计算布林线指标"""
            n = len(close_prices)
            if n < window:
                return {
                    'boll_twenty': np.full(n, np.nan),
                    'upper_rail': np.full(n, np.nan),
                    'lower_rail': np.full(n, np.nan)
                }

            # 计算滑动窗口视图
            close_arr = np.array(close_prices, dtype=np.float64)
            windows = sliding_window_view(close_arr, window)

            # 计算中轨(MA20)
            ma20 = np.mean(windows, axis=1)
            boll_mid = np.concatenate([np.full(window - 1, np.nan), ma20])

            # 计算标准差
            stds = np.std(windows, ddof=1, axis=1)
            stds_full = np.concatenate([np.full(window - 1, np.nan), stds])

            # 计算上下轨
            boll_upper = boll_mid + num_std * stds_full
            boll_lower = boll_mid - num_std * stds_full

            return {
                'boll_twenty': boll_mid,
                'upper_rail': boll_upper,
                'lower_rail': boll_lower
            }

        # 分组并行计算
        def process_group(group):
            group = group.sort_values('stock_date')
            close_prices = group['close_price'].values.astype(float)
            stock_name = group['stock_name'].iloc[0]
            max_date = group['stock_date'].max()
            # 向量化计算布林线
            boll = vectorized_bollinger_bands(close_prices)

            # 组装结果
            group['boll_twenty'] = boll['boll_twenty']
            group['upper_rail'] = boll['upper_rail']
            group['lower_rail'] = boll['lower_rail']
            max_dates.append([stock_name, max_date])
            return group.dropna(subset=['upper_rail'])

        result_df = all_data.groupby('stock_code', group_keys=False).apply(process_group)

        # 准备写入数据
        output_cols = ['stock_code', 'stock_name', 'stock_date',
                       'boll_twenty', 'upper_rail', 'lower_rail']
        result_df = result_df[output_cols].replace({np.nan: None})

        # 批量写入数据库
        if not result_df.empty:
            cnt = my.batch_insert_or_update(engine, result_df, insert_table,
                                            'stock_code', 'stock_date')
            if cnt > 0:
                update_record = pd.DataFrame(max_dates, columns=['stock_code', f'{update_col}'])
                my.batch_insert_or_update(engine, update_record, update_table, 'stock_code')
                print(f"成功更新{batch_names}布林线数据")
    # 记录结束时间
    end_time = time.time()
    # 计算执行时间
    execution_time = end_time - start_time
    print(f"程序执行时间: {execution_time:.6f} 秒")


def calculate_today_stock_cci(frequency, batch_size=10):
    start_time = time.time()
    engine = my.get_mysql_connection()
    stock_list = gs.get_stock_list_for_update_df()

    # 频率配置字典
    freq_config = {
        'd': ('stock_history_date_price', 'stock_date_cci', 'update_stock_date_cci', 'AND tradestatus = 1'),
        'w': ('stock_history_week_price', 'stock_week_cci', 'update_stock_week_cci', ''),
        'm': ('stock_history_month_price', 'stock_month_cci', 'update_stock_month_cci', '')
    }
    select_table, insert_table, update_col, trade_status = freq_config[frequency[0]]
    update_table = 'update_stock_record'
    for i in range(0, len(stock_list), batch_size):
        batch_codes = stock_list.iloc[i:i + batch_size]['stock_code'].unique()
        code_str = ", ".join([f"'{code}'" for code in batch_codes])
        batch_names = stock_list.iloc[i:i + batch_size]['stock_name'].unique()
        print(f'开始计算{batch_names}CCi值')
        # 批量查询SQL（参数化防注入）
        sql = f"""
        SELECT 
            a.stock_code,b.stock_name,a.stock_date,
            a.close_price,b.stock_ma20,rn,
            a.open_price,a.high_price,a.low_price,
            ((a.high_price + a.low_price + a.close_price)/3) as tp
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY stock_code ORDER BY stock_date ASC
            ) rn
            FROM stock.{select_table} 
            WHERE stock_code in ({code_str}) 
            {trade_status}
        ) a
        JOIN date_stock_moving_average_table b 
            ON a.stock_code = b.stock_code 
            AND a.stock_date = b.stock_date;
        """

        # 执行批量查询
        result = my.execute_read_query(engine, sql)
        all_data = pd.DataFrame(result,
                                columns=['stock_code', 'stock_name', 'stock_date', 'close_price', 'stock_ma20', 'rn',
                                         'open_price', 'high_price', 'low_price', 'tp'])
        if all_data.empty:
            print("无有效数据")
            return
        max_dates = []

        def compute_rolling_mad(series, window):
            """向量化计算滚动平均绝对差(MAD)"""
            arr = series.values.astype(float)
            n = len(arr)
            if n < window:
                return np.full(n, np.nan)

            # 生成滑动窗口视图
            windows = sliding_window_view(arr, window)
            # 计算每个窗口的均值
            means = np.mean(windows, axis=1)
            # 计算绝对差并求均值
            mad_values = np.mean(np.abs(windows - means[:, None]), axis=1)
            # 对齐原始数据长度（前window-1天为NaN）
            return np.concatenate([np.full(window - 1, np.nan), mad_values])

        def batch_compute_cci(df_group):
            """批量计算CCI指标"""
            group = df_group.copy()
            # 向量化计算TP（如果SQL未预先计算）
            # group['tp'] = (group['high_price'] + group['low_price'] + group['close_price']) / 3
            code = df_group.name
            max_date = df_group['stock_date'].max()
            # 计算14日SMA
            group['sma14'] = group['tp'].astype(float).rolling(window=14, min_periods=14).mean()

            # 向量化计算MAD
            group['mad'] = compute_rolling_mad(group['tp'], 14)

            # 计算CCI并清理数据
            group['cci'] = (group['tp'].astype(float) - group['sma14']) / (0.015 * group['mad'])
            max_dates.append([code, max_date])
            return group.dropna(subset=['cci'])

        # 并行分组计算
        result_df = all_data.groupby('stock_code', group_keys=False).apply(batch_compute_cci)

        # 准备写入数据
        output_cols = ['stock_code', 'stock_name', 'stock_date', 'tp', 'mad', 'cci']
        result_df = result_df[output_cols].replace({np.nan: None})

        # 批量写入数据库
        if not result_df.empty:
            cnt = my.batch_insert_or_update(engine, result_df, insert_table, 'stock_code', 'stock_date')
            if cnt > 0:
                update_record = pd.DataFrame(max_dates, columns=['stock_code', f'{update_col}'])
                my.batch_insert_or_update(engine, update_record, update_table, 'stock_code')
                print(f'成功更新{batch_names}CCi值')
    # 记录结束时间
    end_time = time.time()
    # 计算执行时间
    execution_time = end_time - start_time
    print(f"程序执行时间: {execution_time:.6f} 秒")


def compute_all_rsi(close_prices, windows=[6, 12, 24]):
    n = len(close_prices)
    if n == 0:
        return {f'rsi_{window}': np.array([]) for window in windows}

    delta = np.zeros(n)
    delta[1:] = np.diff(close_prices)

    gain = np.where(delta > 0, delta, 0.0)
    loss = np.where(delta < 0, -delta, 0.0)

    rsis = {}

    for window in windows:
        if n < window:
            rsi = np.full(n, np.nan)
        else:
            avg_gain = np.zeros(n)
            avg_loss = np.zeros(n)

            # 初始SMA（第window-1天）
            start = window - 1
            avg_gain[start] = np.mean(gain[:window])
            avg_loss[start] = np.mean(loss[:window])

            alpha = (window - 1) / window
            beta = 1 / window

            # 从第window天开始计算EMA（原代码为range(window, n)）
            for i in range(window, n):
                avg_gain[i] = avg_gain[i - 1] * alpha + gain[i] * beta
                avg_loss[i] = avg_loss[i - 1] * alpha + loss[i] * beta

            rs = np.divide(avg_gain, avg_loss, out=np.zeros_like(avg_gain), where=avg_loss != 0)
            rsi = 100 - (100 / (1 + rs))
            rsi[:window - 1] = np.nan  # 仅前window-1天无效

        rsis[f'rsi_{window}'] = rsi

    return rsis


def calculate_stock_rsi(frequency, batch_size=10):
    start_time = time.time()
    # 频率配置字典
    freq_config = {
        'd': ('stock_history_date_price', 'stock_date_rsi', 'update_stock_date_rsi', 'AND tradestatus = 1'),
        'w': ('stock_history_week_price', 'stock_week_rsi', 'update_stock_week_rsi', ''),
        'm': ('stock_history_month_price', 'stock_month_rsi', 'update_stock_month_rsi', '')
    }
    select_table, insert_table, update_col, trade_status = freq_config[frequency[0]]
    engine = my.get_mysql_connection()
    stock_list = gs.get_stock_list_for_update_df()
    record_table = 'update_stock_record'
    for i in range(0, len(stock_list), batch_size):
        batch_codes = stock_list.iloc[i:i + batch_size]['stock_code'].unique()
        batch_names = stock_list.iloc[i:i + batch_size]['stock_name'].unique()
        code_str = ", ".join([f"'{code}'" for code in batch_codes])
        sql = f'''
        SELECT a.stock_code,b.stock_name,a.stock_date,a.close_price 
        FROM (
            SELECT stock_code,stock_date,close_price 
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
        result = my.execute_read_query(engine, sql)
        df = pd.DataFrame(result, columns=['stock_code', 'stock_name', 'stock_date', 'close_price'])
        print(f'开始计算{batch_names}RSI值')
        result_dfs = []
        max_dates = []
        for stock, group in df.groupby('stock_code'):
            group = group.sort_values('stock_date')
            close_prices = group['close_price'].values.astype(float)
            max_date = group['stock_date'].max()
            # 批量计算所有窗口RSI
            rsis = compute_all_rsi(close_prices, windows=[6, 12, 24])
            # 将结果添加到DataFrame
            for window in [6, 12, 24]:
                group[f'rsi_{window}'] = rsis[f'rsi_{window}']

            result_dfs.append(group)
            max_dates.append([stock, max_date])

        final_df = pd.concat(result_dfs)
        final_df = final_df[final_df['rsi_6'].notna()]

        # 替换NaN为None以适应数据库
        rsi_columns = ['rsi_6', 'rsi_12', 'rsi_24']
        final_df[rsi_columns] = final_df[rsi_columns].replace({np.nan: None})

        if len(final_df) > 0:
            cnt = my.batch_insert_or_update(engine, final_df, insert_table, 'stock_code', 'stock_date', batch_size=1500)
            if cnt > 0:
                update_record = pd.DataFrame(max_dates, columns=['stock_name', f'{update_col}'])
                my.insert_or_update(engine, update_record, record_table, 'stock_code')
            print(f'{batch_names}RSI值计算完成')
    # 记录结束时间
    end_time = time.time()
    # 计算执行时间
    execution_time = end_time - start_time
    print(f"程序执行时间: {execution_time:.6f} 秒")


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
    # calculate_stock_macd('d')
    # calculate_stock_macd('w')
    # calculate_stock_macd('m')

    calculate_stock_boll('d')
