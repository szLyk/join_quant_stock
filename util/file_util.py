from datetime import datetime


def write_to_file(file_name, content):
    # 获取当前日期和时间，格式化为 YYYY-MM-DD_HH-MM-SS
    timestamp = datetime.now().strftime('%Y-%m-%d')

    # 构造文件名，例如 '2025-03-14_11-22-33.txt'
    filename = f"../log/{timestamp}_{file_name}.txt"

    # 打开文件（如果文件不存在，则会创建），并以追加模式写入内容
    with open(filename, mode='a', encoding='utf-8') as file:
        file.write(content + '\n')  # 在内容末尾添加换行符以便于阅读

    print(f"内容已成功写入 {filename}")