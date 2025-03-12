from util.get_stock import update_all_stock_today_price, update_all_stock_history_date_week_month_price, \
    get_some_stock_data


list_column = ['stock_code', 'stock_name', 'stock_date', 'close_price', 'stock_ma3', 'stock_ma5', 'stock_ma7', 'stock_ma10', 'stock_ma12', 'stock_ma20', 'stock_ma26', 'stock_ma30', 'stock_ma60', 'stock_ma70', 'stock_ma125', 'stock_ma250', 'stock_week_date', 'stock_month_date']
tow_column = ('stock_code', 'stock_date')
columns_to_update = [col for col in list_column if col not in tow_column]
print(columns_to_update)

