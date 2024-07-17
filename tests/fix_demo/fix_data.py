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
任务简介：


任务流程：


温馨提示：
要把大象装冰箱，总共分几步？
第一步：Models 导出: 配置好数据库后执行
python manage.py inspectdb > server/models.py
第二步：复制 fix_demo 目录，在新目录中创建归档文件夹 ./data_changes/fix_xxx
第三步：编写 rule 规则方法。
"""


class FixXxx(ETLBase):
    """
    Users 表数据清洗
    """

    target_model = Users  # 需要清洗的数据表
    archive_dir = f"{dirname(abspath(__file__))}/data_changes/fix_xxx"  # 当前目录新建的归档目录
    pre_check_mode = True  # 可选，预检查模式（只清洗，不提交）开发阶段建议开启。

    def filter(self) -> models.QuerySet:
        """筛选条件 (可选)"""
        return Users.objects.filter(name="test")

    def rule(self, record: Users):
        """
        清洗规则
        为 username 字段添加 _test 后缀
        """
        Logger.info(f"数据清洗测试 id: {record.id}")

        record.username += "_test"

        return record

    # def test_recover_xxx(self):
    #     """数据恢复"""
    #     self.recover()

    def test_fix_xxx(self):
        """数据清洗"""
        self.start()
