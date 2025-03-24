import os
import inspect
import logging
import platform
from pathlib import Path
from colorlog import ColoredFormatter
from logging.handlers import RotatingFileHandler

# 定义日志的根目录
BASE_DIR = Path(__file__).resolve().parent.parent  # 获取项目根目录
RES_LOG_PATH = BASE_DIR / "log"  # 日志文件夹路径

# 如果日志目录不存在，则创建
if not RES_LOG_PATH.exists():
    RES_LOG_PATH.mkdir(parents=True, exist_ok=True)


def log(name: str = None) -> logging.Logger:
    """
    设置并返回到记录器
    :param name: 如果需要新的记录器，输入logger名字
    :return: logger
    """
    # 重置 根logger 处理程序
    root_logger = logging.getLogger()
    root_logger.handlers = []

    if not name:
        absolute_path = Path(inspect.stack()[1].filename).resolve()
        relative_path = str(absolute_path.relative_to(Path(__file__).resolve().parent.parent))
        spliter = (
            "\\" if platform.system() == "Windows" else "/" if platform.system() == "Linux" else Exception(
                "Incompatible systems")
        )
        name = ".".join([str(n) for n in os.path.splitext(relative_path)[0].split(spliter) if n])

    # 检查 logger 是否存在，如果存在则返回，否则创建
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        return logger
    else:
        # 创建 logger 对象
        logger = logging.getLogger(str(name.split(".")[0]))
        logger.setLevel(logging.DEBUG)

        # 定义 Handler 对象
        file_handler = RotatingFileHandler(os.path.join(RES_LOG_PATH, "x_log.log"), maxBytes=1048576, backupCount=10)
        console_handler = logging.StreamHandler()

        # 设置日志级别
        file_handler.setLevel(logging.DEBUG)
        console_handler.setLevel(logging.DEBUG)

        # 设置输出格式化程序
        file_fmt = "%(asctime)s - %(levelname)s [%(name)s] %(message)s"
        color_fmt = f"%(log_color)s{file_fmt}%(reset)s"
        date_fmt = "%Y-%m-%d %H:%M:%S"

        file_formatter = logging.Formatter(file_fmt)
        console_formatter = ColoredFormatter(
            color_fmt, datefmt=date_fmt, reset=True,
            log_colors={
                "DEBUG": "cyan", "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "red",

            },
        )
        file_handler.setFormatter(file_formatter)
        console_handler.setFormatter(console_formatter)

        # 将 handler 添加到 logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        logger.setLevel(logging.ERROR)

        logger.debug(f"Initialize logger {name}")
        return logging.getLogger(name)
