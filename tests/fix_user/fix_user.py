import os
import traceback
from os.path import join, dirname, abspath

import django
from django.db import models


# 初始化 app 在导入模型之前初始化）
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "data_fix.settings")
django.setup()


from tests.etl import Logger
from tests.etl.etl_base import ETLBase

from server.models import Users

"""
数据清洗案例
"""


class DataFixDemo(ETLBase):
    """
    RouteTask 表数据清洗案例
    """

    target_model = Users
    # archive_dir = join(dirname(abspath(__file__)), "history_records")  # 存储到当前目录
    archive_dir = "/Users/miccolo/etl_test"  # 建议不同的清洗任务存入不同的目录，避免数据被覆盖。
    pre_check_mode = True  # 可选，预检查模式（只清洗，不提交）开发阶段建议开启。

    # def filter(self) -> models.QuerySet:
    #     """筛选条件 (可选)"""
    #     return Users.objects.filter(name__contains="test")

    def rule(self, record: Users):
        """
        清洗规则
        为 username 字段添加 _test 后缀
        """
        Logger.info(f"数据清洗测试 id: {record.id}")

        # if not record.username.endswith("_test"):
        #     record.username += "_test"

        # record.xxx = 777
        # record.yyy = "yyy"
        # record.org_id=2

        return record

    def test_recover_xxx(self):
        """数据恢复"""
        self.recover()

    def test_fix_xxx(self):
        """数据清洗"""
        self.start()
        # self.start(min_id=10)  # 开始的 ID
        # self.start(max_id=20)  # 结束的 ID
        # self.start(min_id=27, max_id=27)  # 同时指定开始和结束
