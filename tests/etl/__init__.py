import logging
import os
from logging.handlers import RotatingFileHandler

# 先初始化 Django 再配置日志，避免 etl 日志配置被 Django 覆盖。
prefix = "[%(asctime)s] %(filename)s:%(lineno)3d [%(levelname)s] %(message)s"
# logging.basicConfig(format=prefix, level=logging.INFO)
formatter = logging.Formatter(prefix)

Logger = logging.getLogger("ETL")
Logger.setLevel(logging.INFO)

# 文件存储路径
# file_dir = os.path.abspath(__file__)
# file_dir = os.path.dirname(file_dir)
# file_dir = os.path.dirname(file_dir)
# log_file = f"{file_dir}/ETL.log"

log_file = f"ETL.log"  # 日志文件，存储在当前执行脚本的目录

rotating = RotatingFileHandler(log_file, maxBytes=1024 * 1024 * 100, backupCount=3)

rotating.setFormatter(formatter)
Logger.addHandler(rotating)

stream = logging.StreamHandler()  # 往屏幕上输出
stream.setFormatter(formatter)  # 设置屏幕上显示的格式
Logger.addHandler(stream)
