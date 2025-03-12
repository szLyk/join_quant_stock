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

    # 将 DataFrame 转换为字典列表
    data_dicts = df.to_dict('records')
    try:
        with engine.connect() as conn:
            for record in data_dicts:
                # 使用 bindparams 绑定参数，确保参数按名称绑定
                stmt = text(sql).bindparams(**record)
                conn.execute(stmt)
            conn.commit()
        return len(df)
    except Error as e:
        print(f"执行更新失败: {e}")

