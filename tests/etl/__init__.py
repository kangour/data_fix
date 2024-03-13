import logging
from logging.handlers import RotatingFileHandler

# 先初始化 Django 再配置日志，避免 etl 日志配置被 Django 覆盖。
test_log_file_path = "ETL.log"
prefix = "[%(asctime)s] %(filename)s:%(lineno)3d [%(levelname)s] %(message)s"
# logging.basicConfig(format=prefix, level=logging.INFO)

Logger = logging.getLogger("ETL")
handler = RotatingFileHandler(test_log_file_path, maxBytes=1024 * 1024 * 100, backupCount=3)
formatter = logging.Formatter(prefix)
handler.setFormatter(formatter)
Logger.addHandler(handler)
