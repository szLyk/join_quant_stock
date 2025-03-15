import pandas
from mysql.connector import Error
from util.read_resource_file import MYSQL_HOST, MYSQL_USER, MYSQL_USER_PASSWORD, MYSQL_DATABASE
from sqlalchemy import text, create_engine


def execute_read_query(engine, query, params=None):
    try:
        with engine.connect() as connection:
            result_proxy = connection.execute(text(query), params)
            results = result_proxy.fetchall()
        print("查询执行成功")
        return results
    except Error as e:
        print(f"读取数据失败: {e}")


def execute_query(engine, query, params=None):
    try:
        with engine.connect() as connection:
            result_proxy = connection.execute(text(query), params)
            connection.commit()
        print("SQL执行成功")
        return result_proxy.rowcount  # 返回受影响的行数
    except Error as e:
        print(f"查询执行失败: {e}")


# 连接MySQL
def get_mysql_connection():
    return create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_USER_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}')


# 创建SQL更新插入语句
def get_upsert_sql(df, no_update_columns):
    columns_to_update = [col for col in df.columns if col not in no_update_columns]
    return ', '.join([f'{col}=VALUES({col})' for col in columns_to_update])


def insert_or_update(engine, df, table_name, *no_update_columns):
    # 首先，构建 INSERT INTO 语句
    columns = ', '.join(df.columns)
    placeholders = ', '.join([f':{col}' for col in df.columns])
    updates = get_upsert_sql(df, set(no_update_columns))

    sql = f'''
    INSERT INTO {table_name} ({columns})
    VALUES ({placeholders})
    ON DUPLICATE KEY UPDATE {updates}
    '''

    # 将 DataFrame 转换为列表的元组
    data_dicts = df.to_dict('records')

    try:
        with engine.connect() as conn:
            # 使用 executemany 方法进行批量插入/更新
            conn.execute(text(sql), data_dicts)
            conn.commit()
        print(f"成功插入或更新 {len(df)} 条记录")
        return len(df)
    except Exception as e:  # 更推荐捕获更具体的异常类型
        print(f"执行插入或更新失败: {e}")
        return 0


def batch_insert_or_update(engine, df, table_name, *no_update_columns, batch_size=2000):
    total_records = len(df)
    processed_records = 0

    for i in range(0, total_records, batch_size):
        batch_df = df.iloc[i:i + batch_size]
        processed = insert_or_update(engine, batch_df, table_name, *no_update_columns)
        processed_records += processed

        print(f"已处理第 {i // batch_size + 1} 批次，共 {min(i + batch_size, total_records)} 条记录")

    return processed_records
