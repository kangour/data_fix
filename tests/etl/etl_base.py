import enum
import json
import os
import shutil
import time
import traceback
import unittest
from datetime import datetime
from itertools import chain

import oss2
from django.db import models
from django.db.models import Max

import config
from tests.etl import Logger

"""
Tip:
一个 ETL 子类只允许清洗一个表，以便使用归档和恢复功能（支持但表的一个或者多个字段）
"""


# ETL(Extract-Transform-Load) 即抽取-转换-加载队列数量 todo 一期仅用单线程，未来再用队列异步提交
# extract_queue = Queue(100)
# transform_queue = Queue(100)
# load_queue = Queue(100)

auth = oss2.Auth(config.OssConfig.access_id, config.OssConfig.access_key)
# bucket = oss2.Bucket(auth, "http://oss-cn-hangzhou.aliyuncs.com", "xyi-mobile")
bucket = oss2.Bucket(
    auth, config.OssConfig.oss_endpoint, config.OssConfig.oss_bucket_name
)


def to_origin_dict(obj: models.Model) -> dict:
    """
    orm 对象转为完整的字典，临时 提供给未实现软删除的类使用，未来所有表都继承了 SoftDeleteBaseModel 后，这里将删除。
    """
    opts = obj._meta
    data = {}
    for f in chain(opts.concrete_fields, opts.private_fields):
        data[f.name] = f.value_from_object(obj)
    for f in opts.many_to_many:
        data[f.name] = [i.id for i in f.value_from_object(obj)]
    return data


class ArchiveSceneEnum(enum.Enum):
    """
    数据归档场景
    """

    data_fix = "data_fix"  # 字段修正
    data_recover = "data_recover"  # 字段恢复


class ETLBase(unittest.TestCase):
    """
    目标：
    分页数据清洗: 根据 page_size 属性，分页查询，批量提交。
    原始数据归档：记录原始数据的所有字段。
    变更历史归档：只记录变更的字段在清洗前后的值。
    归档数据恢复：基于原始数据或者变更历史将数据重写都数据库。
    归档文件上传：清洗完成或者恢复完成后，将归档文件打包上传到 OSS 中。
    批量提交： todo：
    队列 + 多线程异步清洗： todo
    预检查模式：如果 pre_check_mode 属性值为 True，则只运行清洗过程，但不提交到数据库，以便于提前找出脏数据或者清洗规则的错误。
    """

    target_model = models.Model  # 必填，表模型
    archive_dir = None  # 必填，归档目录，不同的清洗任务，请放在不同的目录
    pre_check_mode = False  # 可选，是否为预检查模式 (只在本地验证清洗逻辑，不提交到数据库)
    page_size = 100  # 可选，分页尺寸

    # worker: int  # 多线程数量 todo 一期仅用单线程

    def __init__(self, *args, **kwargs):
        super(ETLBase, self).__init__(*args, **kwargs)
        self.database = config.DBConfig.database  # 为了避免传输错误，不从外部传入，而是直接读配置
        self._check_config()
        self.table_name = self.target_model._meta.db_table  # Meta 中定义的 db_table

        # 归档路径创建
        self.database_archive_dir = (
            f"{self.archive_dir}/{self.database}/{self.table_name}"
        )
        self._archive_dir_init()

        # 清洗前 原始数据
        self.origin_file = (
            f"{self.database_archive_dir}/{self.database}@{self.table_name}__origin.txt"
        )
        # 清洗时 字段变更
        self.change_field_file = f"{self.database_archive_dir}/{self.database}@{self.table_name}__change_field.txt"
        # 清洗后 完整数据变更
        self.changed_file = f"{self.database_archive_dir}/{self.database}@{self.table_name}__changed.txt"
        # 恢复后 完整数据变更
        self.recovered_file = f"{self.database_archive_dir}/{self.database}@{self.table_name}__recovered.txt"

        Logger.info(
            f"归档路径: {self.database_archive_dir}/{self.database}@{self.table_name}__*.txt"
        )

    def filter(self) -> models.QuerySet:
        """查询条件，子类可以重写 filter 方法，加入自己的条件"""
        return self.target_model.objects.filter()

    def rule(self, record: models.Model):
        """
        数据清洗规则，子类需要继承后实现 rule 方法，子类只需要给 record 赋值，无需执行 save()
        :param record: 待清洗的记录
        :return: 清洗后的记录

        清洗前的完整数据自动记录到 __origin.txt 文件
        清洗的变更自动记录到 __change_field.txt 文件
        清洗后的完整数据自动记录到 __changed.txt 文件
        """
        raise NotImplemented(f"尚未实现清洗规则")

    def start(self, min_id: int = 1, max_id: int = None):
        """
        启动数据订正/清洗
        """
        _max_id = self.target_model.objects.aggregate(Max("id"))["id__max"]
        if max_id is None:
            max_id = _max_id
        if max_id > _max_id:
            max_id = _max_id

        if min_id > max_id:
            raise Exception(f"记录 id 范围错误，min_id: {min_id} 应小于 max_id: {max_id}")
        if self.page_size < 1:
            raise Exception(f"page_size 不能小于 1。")

        Logger.info(f"数据清洗 {self.database}@{self.table_name}.id 范围：[{min_id}, {max_id}]")

        waiting_count = self.filter().filter(id__gte=min_id, id__lte=max_id).count()
        Logger.info(f"符合条件，即将清洗的数据有：{waiting_count} 条")

        # 分页查询，逐条清洗
        data_count = 0
        offset = min_id

        filters = self.filter()
        while True:
            # 查询 page_size 条数据
            # 按 id 排序，用切片查询确保每次都能拿到足量数据
            # 记录最新的 id 偏移量继续用 page_size 进行切片分页。
            records = filters.filter(id__gte=offset, id__lte=max_id).order_by("id")[: self.page_size]
            if not records:
                break

            if not isinstance(records[0], self.target_model):
                raise Exception(
                    f"查询的数据类型 {type(records[0])} 与模型属性 {type(self.target_model)} 不一致。"
                )

            offset = list(records)[-1].id + 1

            for record in records:
                # record: self.target_model
                try:
                    # 记录清洗前的完整数据
                    origin_data = to_origin_dict(record)
                    self._save_origin(record.id, origin_data)

                    # 调用清洗规则
                    _record = self.rule(record)
                    if not _record:
                        _record = record
                    else:
                        if not isinstance(_record, self.target_model):
                            raise Exception(
                                f"返回的记录类型 {type(_record)} 与模型属性 {type(self.target_model)} 不一致。"
                            )
                        if _record.id != record.id:
                            raise Exception(f"返回的记录 id 与输入记录 id 不一致。")

                    changed_data = to_origin_dict(_record)

                    has_field_changed = False
                    for field_name, field_value in origin_data.items():
                        changed_value = changed_data[field_name]
                        if field_value != changed_value:
                            _field_meta = _record._meta.get_field(field_name)
                            if hasattr(_field_meta, "auto_now"):
                                if _field_meta.auto_now is True:
                                    raise Exception(
                                        f"请勿在规则方法中主动调用 save() 方法，因为 ETL 父类会统一处理提交，目前 {field_name} 已发生变化，可能会存在重复提交，影响效率。"
                                    )
                            has_field_changed = True
                            # 保存字段变更
                            self._save_change_field(
                                _record,
                                field_name=field_name,
                                origin_value=field_value,
                                target_value=changed_value,
                            )

                    # todo 放入队列，异步提交
                    if has_field_changed:
                        if self.pre_check_mode is False:
                            _record.save()
                            changed_data = to_origin_dict(_record)  # auto now 字段变了，重新读取
                            # 保存记录数据变更
                            self._save_changed(_record, origin_data, changed_data)
                        data_count += 1

                except Exception as e:
                    traceback.print_exc()
                    Logger.warning(
                        f"修正失败，请检查记录 {self.database}@{self.table_name}.id={record.id} 异常信息：{e}"
                    )
                    return

        Logger.info(f"清洗完成：共 {data_count} 条记录")
        if self.pre_check_mode:
            Logger.warning(f"预检模式已开启，未提交数据。")
        if data_count > 0:
            # 归档文件上传到 oss
            self._archive_to_oss(ArchiveSceneEnum.data_fix.value)

    def recover(self):
        """
        数据恢复
        """
        # 基于字段变更进行恢复
        self._recover_change_field()

        # # 基于原始记录恢复
        # self._recover_origin()

        # # 基于变更的记录恢复
        # self._recover_changed()

    def _recover_change_field(self):
        """
        变更文件 __change_field.txt 数据恢复
        todo mysql 5.7 json 字段的恢复测试
        todo 批量提交
        """
        Logger.info(f"根据字段变更恢复: {self.change_field_file}")
        recover_count = 0
        for meta in self._archive_iter(self.change_field_file):
            database = meta["database"]
            table_name = meta["table_name"]
            record_id = meta["record_id"]
            field_name = meta["field_name"]
            archive_origin_value = meta["origin_value"]
            archive_target_value = meta["target_value"]
            archive_note = meta["note"]

            if archive_origin_value is not None and archive_target_value is not None:
                if type(archive_origin_value) != type(archive_target_value):
                    raise Exception(
                        f"恢复失败：变更的数据类型 {archive_target_value} {type(archive_target_value)} 与原始数据类型 {archive_origin_value} {type(archive_origin_value)}  不一致。"
                    )

            model = self.target_model
            if not field_name:
                raise Exception(f"恢复变更时，字段名不能为空: {meta}")
            # 库的校验
            if self.database != database:
                raise Exception(f"归档文件的数据库 {database} 与配置 {self.database}不匹配")
            if model._meta.db_table != table_name:
                raise Exception(
                    f"模型 db_table={model} 与配置 table_name={table_name} 不匹配，请检查 ETL 表配置或者 ORM 的定义"
                )
            if not hasattr(model, field_name):
                raise Exception(f"模型 {model} 中不存在字段 {field_name}")

            # Logger.info(
            #     f"开始恢复: {self.database}@{table_name}.id={record_id} {field_name}={archive_origin_value}"
            # )

            try:
                record: model = model.objects.get(id=record_id)
            except model.DoesNotExist:
                Logger.warning(
                    f"一个变更恢复失败，记录可能已被删除：{self.database}@{table_name}.id={record_id}，变更: {field_name}={archive_origin_value}"
                )
                Logger.warning(f"对于被删除的数据，可以通过原始文件进行恢复，参照 _recover_origin() 方法。")
                continue
            # values = to_origin_dict(record)
            origin_value = getattr(record, field_name)
            # origin_value = record.__getattribute__(field_name)
            if origin_value == archive_origin_value:
                Logger.warning(
                    f"重复恢复，跳过: {self.database}@{table_name}.id={record_id} {field_name}={archive_origin_value}"
                )
                continue
            if origin_value != archive_target_value:
                Logger.warning(
                    f"数据已发生变化，可能来自外部编辑，本次将继续恢复。库中的期望值: {archive_target_value} 实际的值: {origin_value} 计划写入的原始值: {archive_origin_value}"
                )
            # 恢复前，记录完整数据
            origin_data = to_origin_dict(record)

            record.__setattr__(field_name, archive_origin_value)
            # todo 放入队列，异步提交
            if self.pre_check_mode is False:
                record.save()
                # 恢复后，记录完整数据
                target_data = to_origin_dict(record)
                self._save_recovered(record, origin_data, target_data)

            Logger.info(
                f"恢复成功: {self.database}@{table_name}.id={record_id} 字段 {field_name}={archive_origin_value}"
            )
            recover_count += 1

        # 归档文件上传到 oss
        Logger.info(f"恢复完成：共 {recover_count} 次字段变更")
        if recover_count > 0:
            self._archive_to_oss(ArchiveSceneEnum.data_recover.value)

    def _archive_dir_init(self):
        """
        归档目录初始化
        """
        # 检查 archive_dir 是否存在
        if not os.path.exists(self.archive_dir):
            raise Exception(
                f"数据归档根目录 {self.archive_dir} 不存在，为避免出错，请人工创建该目录，不同的清洗任务请使用不同的归档目录。"
            )

        # 为数据库创建归档目录
        if not os.path.exists(self.database_archive_dir):
            os.makedirs(self.database_archive_dir)
            Logger.info(f"创建数据归档目录：{self.database_archive_dir}")
            time.sleep(1)  # 等待 1 秒，以免文件夹创建失败，造成文件追加错误

            # 再次检查 self.database_archive_dir 是否存在
            if not os.path.exists(self.database_archive_dir):
                raise Exception(
                    f"数据归档目录 {self.database_archive_dir} 不存在，自动创建失败，请检查权限或者人工创建。"
                )

    def _check_config(self):
        """
        配置检查
        """
        if not self.archive_dir:
            raise Exception("请设置 cls.archive_dir 属性")
        if not self.database:
            raise Exception("请设置 cls.database 属性")
        if not self.target_model:
            raise Exception("cls.target_model 不能为空")
        if not isinstance(self.target_model(), models.Model):
            raise Exception("请设置正确的 cls.target_model 属性")
        if not self.target_model._meta.db_table:
            raise Exception(f"请为 {self.target_model} 设置 Meta.db_table 属性")
        # if not self.field_name:
        #     raise Exception("请设置 cls.field_name 属性")
        if not self.page_size:
            raise Exception("请设置 cls.page_size 属性")

        # 软删除暂时 不强制。
        # if not issubclass(self.target_model(), SoftDeleteBaseModel):
        #     raise Exception(f"{self.target_model} orm model 必须继承自 SoftDeleteModel")
        # if not hasattr(self.target_model, "to_origin_dict"):
        #     raise Exception(f"{self.target_model} orm model 必须继承自 SoftDeleteModel")

    def _record_to_file(
        self,
        archive_file: str,
        record_id: int,
        field_name: str,
        origin_value: any,
        target_value: any,
        note: str = "",
    ):
        """
        在归档文件末尾追加内容，按 database 分组存放
        todo 写入 队列，批量写到文件
        """

        # 数据检查
        if not archive_file:
            raise Exception("archive_file 参数不能为空")
        if not record_id:
            raise Exception("record_id 参数不能为空")
        # 单个字段值变更，才检查字段名
        if archive_file == self.change_field_file:
            if not field_name:
                raise Exception("field_name 参数不能为空")

        # # 这两个判断，未来可以注释，用于支持 None 值归档。
        # if origin_value is None:
        #     raise Exception(f"原始值 origin_value 为空，请检查。如果确认为空，请临时注释这个判断")
        # if target_value is None:
        #     raise Exception(f"目标值 target_value 为空，请检查。如果确认为空，请临时注释这个判断")

        meta = dict(
            database=self.database,
            table_name=self.table_name,
            record_id=record_id,
            field_name=field_name,
            origin_value=origin_value,
            target_value=target_value,
            note=note,
        )
        _meta = json.dumps(meta, default=str, ensure_ascii=False)

        # 检查 meta 是否已经在目标归档文件 archive_file 中 todo 未来用缓存或者数据表查重
        if archive_file == self.change_field_file:
            if os.path.exists(archive_file):
                with open(archive_file, "r") as f:
                    for line in f:
                        line = line.strip()
                        if line == _meta:
                            # Logger.info(f"归档文件 {archive_file} 中已存在记录，跳过写入。")
                            return

        if self.pre_check_mode:
            return

        with open(archive_file, "a+") as f:
            f.write(f"{_meta}\n")

    def _save_origin(self, record_id: int, data: dict):
        """
        记录清洗之前的完整数据
        """
        if not data:
            raise Exception(f"记录 {record_id} 的完整字段数据为空，无法归档，请检查。")
        self._record_to_file(
            archive_file=self.origin_file,
            record_id=record_id,
            field_name="",
            origin_value=data,
            target_value=None,
            note=f"清洗前的完整数据",
        )

    def _save_changed(
        self, changed_record: models.Model, origin_record: dict, target_record: dict
    ):
        """存储数据清洗的记录变更信息"""
        return self._save_record_history(
            self.changed_file, changed_record, origin_record, target_record
        )

    def _save_recovered(
        self, changed_record: models.Model, origin_record: dict, target_record: dict
    ):
        """存储数据恢复的记录变更信息"""
        return self._save_record_history(
            self.recovered_file, changed_record, origin_record, target_record
        )

    def _save_record_history(
        self,
        archive_file: str,
        changed_record: models.Model,
        origin_record: dict,
        target_record: dict,
    ):
        """
        保存记录变更信息
        """
        origin_id = origin_record.get("id")
        target_id = target_record.get("id")

        if not isinstance(changed_record, self.target_model):
            raise Exception(f"model 必须是 {self.target_model} 类型")
        if not origin_record:
            raise Exception(f"origin_record 参数不能为空")
        if not target_record:
            raise Exception(f"target_record 参数不能为空")
        if changed_record.id != origin_id:
            raise Exception(
                f"changed_record.id {changed_record.id} 与 origin_record.id {origin_id} 不一致"
            )
        if changed_record.id != target_id:
            raise Exception(
                f"changed_record.id {changed_record.id} 与 target_record.id {target_id} 不一致"
            )

        self._record_to_file(
            archive_file=archive_file,
            record_id=changed_record.id,
            field_name="",
            origin_value=origin_record,
            target_value=target_record,
            note=f"完整数据的变更",
        )

    def _save_change_field(
        self, changed_record, field_name: str, origin_value: any, target_value: any
    ):
        """
        记录变更
        """

        if origin_value is not None and target_value is not None:
            if type(origin_value) != type(target_value):
                raise Exception(
                    f"归档失败：变更的数据类型 {target_value} {type(target_value)} 与原始数据类型 {origin_value} {type(origin_value)}  不一致。"
                )
        if target_value is None:
            if changed_record._meta.get_field(field_name).null is False:
                raise Exception(
                    f"{field_name} 字段不允许为空: {self.database}@{self.table_name}.id={changed_record.id}"
                )
        if not isinstance(changed_record, self.target_model):
            raise Exception(f"model 必须是 {self.target_model} 类型")
        if not field_name:
            raise Exception(f"存储变更时，字段名不能为空")
        if not hasattr(changed_record, field_name):
            raise Exception(
                f"当前记录 {changed_record}{type(changed_record)} 没有 {field_name} 字段，请检查配置"
            )

        note = f"字段变更：{self.database}@{self.table_name}.id={changed_record.id} 字段 {field_name}={origin_value} 调整为 {field_name}={target_value}"
        Logger.info(f"{note}")
        self._record_to_file(
            archive_file=self.change_field_file,
            record_id=changed_record.id,
            field_name=field_name,
            origin_value=origin_value,
            target_value=target_value,
            note=f"{note}",
        )

    def _archive_iter(self, archive_file: str):
        """
        归档数据迭代器
        """

        # 检查 archive_dir 是否存在
        if not os.path.exists(self.archive_dir):
            raise Exception(f"数据归档根目录 {self.archive_dir} 不存在。")

        # 检查 self.database_archive_dir 是否存在
        if not os.path.exists(self.database_archive_dir):
            raise Exception(f"数据归档目录 {self.database_archive_dir} 不存在。")

        # 检查 archive_file 是否存在
        if not os.path.exists(archive_file):
            raise Exception(f"数据归档文件 {archive_file} 不存在。")

        # 读取文件
        # Logger.info(f"归档文件数据恢复: {archive_file}")
        line_no = 0
        with open(archive_file, "r") as f:
            for line in f:
                line_no += 1
                line = line.strip()
                if not line:
                    raise Exception(f"归档文件 {archive_file}:{line_no} 中存在空行。")
                meta = json.loads(line)
                if not meta["database"]:
                    raise Exception(f"归档文件 {archive_file}:{line_no} 数据库名为空。")
                if not meta["table_name"]:
                    raise Exception(f"归档文件 {archive_file}:{line_no} 表名为空。")
                if not meta["record_id"]:
                    raise Exception(f"归档文件 {archive_file}:{line_no} 记录 ID 为空。")
                if archive_file == self.change_field_file:
                    if not meta["field_name"]:
                        raise Exception(f"归档文件 {archive_file}:{line_no} 字段名称为空。")
                # if meta["origin_value"] is None:
                #     raise Exception(f"归档文件 {archive_file}:{line_no} 原始值为空，请提供默认值。")
                yield meta

    def _recover_origin(self):
        """
        基于完整数据文件 __origin.txt 恢复数据
        """
        Logger.info(f"根据原始数据恢复: {self.origin_file}")
        self._recover_record(self.origin_file)

    def _recover_changed(self):
        """
        基于清洗后的数据文件 __changed 恢复数据
        基于清洗后的数据恢复（只恢复清洗过的记录，相当于将已经洗好的数据再提交一次）
        """
        Logger.info(f"根据记录变更恢复: {self.changed_file}")
        self._recover_record(self.changed_file)

    def _recover_record(self, archive_file: str):
        """
        基于完整数据文件恢复
        todo 未来存储到数据表后，逆序读取逆序数据恢复。
        todo 批量提交
        """
        recover_count = 0
        for meta in self._archive_iter(archive_file):
            database: str = meta["database"]
            table_name: str = meta["table_name"]
            record_id: int = meta["record_id"]
            field_name: str = meta["field_name"]
            archive_origin_value: dict = meta["origin_value"]
            archive_target_value: dict = meta["target_value"]
            archive_note: str = meta["note"]

            model = self.target_model
            # 库的校验
            if self.database != database:
                raise Exception(f"归档文件的数据库 {database} 与配置 {self.database}不匹配")
            if model._meta.db_table != table_name:
                raise Exception(
                    f"模型 db_table={model} 与配置 table_name={table_name} 不匹配，请检查 ETL 表配置或者 ORM 的定义"
                )

            if record_id != archive_origin_value.get("id"):
                raise Exception(
                    f"恢复数据的 record_id {record_id} 与完整数据的 id {archive_origin_value.get('id')} 不一致，请检查归档文件"
                )

            Logger.info(
                f"开始恢复: {self.database}@{table_name}.id={record_id}={archive_origin_value}"
            )

            # todo 记录归档文件

            for field_name in archive_origin_value.keys():
                if not hasattr(model, field_name):
                    raise Exception(f"表字段未找到 {model} 没有 {field_name} 字段，请检查配置")

            # 基于已有数据更新
            records: models.QuerySet = model.objects.filter(id=record_id)
            if len(records) > 1:
                raise Exception(f"恢复数据的 record_id 存在多条记录，请检查数据库")

            # todo 放入队列，异步提交
            if records:
                # 还原旧数据
                records.update(**archive_origin_value)
                Logger.info(f"恢复成功: {self.database}@{table_name}.id={record_id}")
            else:
                # 创建新数据
                record = model(**archive_origin_value)
                if self.pre_check_mode is False:
                    record.save()
                Logger.info(f"创建成功: {self.database}@{table_name}.id={record_id}")

            recover_count += 1

        # 归档文件上传到 oss
        if recover_count > 0:
            Logger.info(f"成功恢复：共 {recover_count} 条记录")
        else:
            Logger.info(f"未恢复任何数据")

    def _zip_archive(self):
        """
        zip 打包 archive_dir 目录，生成 archive-yy-mm-dd.zip 文件。
        """
        src_dir = f"{self.archive_dir}/{self.database}/{self.table_name}"
        if not os.path.exists(src_dir):
            Logger.warning(f"目录 {src_dir} 不存在，请检查配置")
            return

        # 获取 src_dir 的同级目录
        zip_dir = os.path.dirname(src_dir)

        out_file = f"archive-{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        out_file = os.path.join(zip_dir, out_file)
        if os.path.exists(out_file):
            Logger.warning(f"文件 {out_file} 已存在")
            return

        Logger.info(f"开始打包目录 {src_dir} 为 {out_file}.zip")
        shutil.make_archive(out_file, format="zip", root_dir=src_dir, base_dir=src_dir)

        filesize = os.path.getsize(f"{out_file}.zip")
        filesize /= 1024  # kb
        Logger.info(f"打包完成，{filesize} kb")
        return f"{out_file}.zip"

    def _archive_to_oss(self, scene: str):
        """
        归档文件上传到 OSS，访问路径：
        如果配置的 bucket name 是 express-image
        https://oss.console.aliyun.com/bucket/oss-cn-hangzhou/express-image/object?path=data_fix%2F
        """
        # 预检模式，不上传
        if self.pre_check_mode:
            return

        if not scene:
            raise Exception(f"请传入数据变更场景。")

        if config.OssConfig.auto_upload_oss is not True:
            return

        zip_file = self._zip_archive()
        name = os.path.basename(zip_file)
        user = os.getenv("USER") or "developer"
        name = f"data_fix/archive-{datetime.now().strftime('%Y-%m-%d')}/{self.database}/{self.table_name}/{user}-{scene}-{name}"
        # 上传到 oss
        try:
            result = bucket.put_object_from_file(name, zip_file)
            if result.status != 200:
                Logger.warning(
                    f"归档文件 {name} 上传到 oss 失败: {result.request_id} {result.status}"
                )
            else:
                Logger.info(f"归档文件 {name} 成功上传到 oss")
                Logger.info(
                    f"归档文件访问地址：https://oss.console.aliyun.com/bucket/oss-cn-hangzhou/express-image/object?path=data_fix%2F"
                )
        except Exception as e:
            Logger.warning(f"归档文件上传到 OSS 异常: {e}")
