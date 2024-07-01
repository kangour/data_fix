# data_fix

在开发过程中，有时候需要对脏数据做清洗，对数据变更做订正。

但操作过程中，如果只是简单的执行 SQL 语句或者编写简单的数据修改脚本，可能会有错误的数据操作逻辑或者异常，导致数据丢失。

**data_fix** 是一个数据库脏数据清洗工具，错误数据订正工具，ETL 数据转换工具。

提供了自动分页分批查数据，自动记录数据变更，自动提交，自动恢复，自动归档备份，自动上传到 OSS 等能力。

支持 mysql, postgresql, oracle, sqlite3 等数据库，理论上它支持任意数据库！


### 运行环境
- python3
- Django


### 安装依赖

```bash
pip install -r requirements.txt
# pip freeze >> requirements.txt
```

### 准备工作

开始编写之前，使用 Django inspectdb 工具自动生成目标数据库的模型，理论上支持将任意数据库。

首先将 config.py.sample 复制到 config.py 并填写配置信息。

然后之行下面的 inspectdb 语句，得到数据表模型。

模型存储路径: server/models.py

> models.py 名称可以自定义。

```shell
python manage.py inspectdb > server/models.py
```

### 编写数据清洗脚本

## 快速入门

ETLBase 类是本项目的核心，ETLBase 使数据清洗过程变得简单、可逆、高效、安全。

```python
from tests.etl.etl_base import ETLBase
from server.models import Users


class DataFixDemo(ETLBase):

    target_model = Users
    archive_dir = "/Users/xxx/etl_archive"

    def rule(self, record: Users):
        record.name = "test"

    def test_fix_xxx(self):
        self.start()
```


简单四步，获得一个数据清洗脚本：
1. 继承 ETLBase 父类
2. 填写类属性
  a. target_model  # 必填，需要清洗的表模型
  b. archive_dir  # 必填，归档目录
3. 实现规则方法
  a. rule() 方法是父类指定的数据清洗规则方法，需要在子类中实现，父类会将 model 对象传给 rule 方法，更改记录的值后，无需执行 save()，父类会自动记录变更并提交。
4. 调用父类 start() 方法

通过以上四步，就拥有了一个支持自动归档，自动恢复，自动分页查询，自动提交的数据清洗脚本。

### 一个简单的例子

数据清洗要求：将 Users 表的 name 字段，全加上 test 后缀。

```python
from tests.etl.etl_base import ETLBase
from server.models import Users


class FixUsername(ETLBase):
    """
    Users 表数据清洗案例
    """

    target_model = Users
    archive_dir = "/Users/xxx/etl_archive"

    def rule(self, record: Users):
        """
        清洗规则
        为 name 字段添加 _test 后缀
        """
        if not record.name.endswith("_test"):
            record.name += "_test"
        return record

    def test_fix_name(self):
        """数据清洗"""
        self.start()
```

在这个 FixUsername 类中，提供了 target_model=Users 属性，表示要清洗的表是 Users。
rule 方法中，为 name 加上 _test 后缀，自定义的 rule 中做了幂等检查，同时参数 record 是父类基于target_model 表和 filter 方法自动查询的数据，无需自己查询。更改属性值后，无需执行 save 方法。父类会自行处理数据变更归档和提交入库。
test_fix_name 方法中，调用了父类的 start() 方法，开始数据清洗。

### 预检查模式

当为子类提供 pre_check_mode = True 属性后，ETL 进入预检查模式，此时的脚本运行后，只会校验数据清洗的逻辑是否正确，但不会真正提交到数据库中。每次开发清洗脚本时，强烈推荐开启。

```python
from tests.etl.etl_base import ETLBase
from server.models import Users


class FixUsername(ETLBase):
    target_model = Users
    archive_dir = "/Users/xxx/etl_archive"
    """预检模式"""
    pre_check_mode = True  # 可选，预检查模式（只清洗，不提交）

    def rule(self, record: Users): pass

    def test_fix_name(self, record: Users): pass
```

### 自定义条件筛选

父类提供了一个 filter() 方法，用于条件筛选，子类可以重写这个方法。
有时候，并不是所有数据都要查出来进行清洗，比如只需要把名称中不包含 test 的记录查出来进行清洗。
可以在子类中实现一个自己的 filter 方法。

```python
from django.db import models
from tests.etl.etl_base import ETLBase
from server.models import Users


class FixUsername(ETLBase):
    target_model = Users
    archive_dir = "/Users/xxx/etl_archive"
    pre_check_mode = True  # 可选，预检查模式（只清洗，不提交）
    page_size = 100  # 可选，分页尺寸
    
    def filter(self) -> models.QuerySet:
        """筛选条件 (可选)"""
        return Users.objects.filter().exclude(name__contains="test")

    def rule(self, record: Users): pass

    def test_fix_name(self, record: Users): pass
```

### 实现清洗规则

在清洗规则中，参数是父类自动查询的 target_model 对象，修改过程中，应尽量处理重复清洗的幂等性，支持同时更改多个属性，相关属性变更都会被父类自动记录，在恢复阶段，更改的所有属性都会被恢复。

```python
from django.db import models
from tests.etl.etl_base import ETLBase
from server.models import Users


class FixUsername(ETLBase):
    target_model = Users
    archive_dir = "/Users/xxx/etl_archive"
    pre_check_mode = True  # 可选，预检查模式（只清洗，不提交）
    page_size = 100  # 可选，分页尺寸
    
    def filter(self) -> models.QuerySet: pass

    def rule(self, record: Users):
        """规则方法中，支持同时更改多个属性"""
        record.name = "test"
        record.org_id = 1

    def test_fix_name(self, record: Users): pass
```

### 自定义 ID 范围

当需要明确一个 ID 范围时，start 方法提供了 min id 和 max id 参数，用于指定 ID 范围。
可以用于小范围测试，或者需要指定 ID 从 某个值开始，避免二次清洗。

```python
from django.db import models
from tests.etl.etl_base import ETLBase
from server.models import Users


class FixUsername(ETLBase):
    target_model = Users
    archive_dir = "/Users/xxx/etl_archive"
    pre_check_mode = True  # 可选，预检查模式（只清洗，不提交）
    page_size = 100  # 可选，分页尺寸
    
    def filter(self) -> models.QuerySet: pass

    def rule(self, record: Users): pass

    def test_fix_name(self, record: Users):
        self.start()
        """指定 ID 的最小值和最大值"""
        # self.start(min_id=100)
        # self.start(min_id=1, max_id=10)
```

### 自定义分页尺寸

通过 page_size 属性，可以配置数据清洗时，单次查询的数据量（默认为 100 条）

```python
from django.db import models
from tests.etl.etl_base import ETLBase
from server.models import Users


class FixUsername(ETLBase):
    target_model = Users
    archive_dir = "/Users/xxx/etl_archive"
    pre_check_mode = True  # 可选，预检查模式（只清洗，不提交）
    """分页尺寸"""
    page_size = 100  # 可选
    
    def filter(self) -> models.QuerySet: pass
        
    def rule(self, record: Users): pass

    def test_fix_name(self, record: Users): pass
```

### 数据恢复

通过父类的 recover 方法，可以实现数据快速恢复，无需额外参数。
当执行完 rule 方法后，数据将自动对比差异进行归档。数据恢复使用的数据是数据清洗时自动归档的数据，无需额外的开发工作。

```python
from django.db import models
from tests.etl.etl_base import ETLBase
from server.models import Users


class FixUsername(ETLBase):
    target_model = Users
    archive_dir = "/Users/xxx/etl_archive"
    pre_check_mode = True  # 可选，预检查模式（只清洗，不提交）
    page_size = 100  # 可选，分页尺寸
    
    def filter(self) -> models.QuerySet: pass

    def rule(self, record: Users): pass

    def test_recover_name(self):
        """数据恢复"""
        self.recover()

    def test_fix_name(self, record: Users): pass
```

### 完整案例

```python
from django.db import models
from tests.etl.etl_base import ETLBase
from server.models import Users
from tests.etl import Logger


class FixUsername(ETLBase):
    """
    Users 表数据清洗案例
    """

    target_model = Users
    archive_dir = "/Users/jietan/etl_test"
    # pre_check_mode = True  # 可选，预检查模式（只清洗，不提交）

    def filter(self) -> models.QuerySet:
        """筛选条件 过滤掉无需清洗的数据"""
        return Users.objects.filter().exclude(name__contains="test")

    def rule(self, record: Users):
        """
        清洗规则
        为 name 字段添加 _test 后缀
        """
        Logger.info(f"数据清洗测试 id: {record.id}")

        record.username += "_test"

        return record

    def test_recover_name(self):
        """数据恢复"""
        self.recover()

    def test_fix_name(self):
        """数据清洗"""
        self.start()
        # self.start(min_id=10)
        # self.start(max_id=20)
        # self.start(min_id=27, max_id=27)


```

## 其他特性

### 前置后后置方法

由于本工具基于测试用例，所以可以使用前置和后置方法 setUp(), tearDown() 做一些额外的处理，比如在数据清洗前，需要查询一些特殊的数据，活着对数据进行一些处理，对数据进行加密，或者对数据进行一些校验等。


### 数据上传

数据清洗结束后，会将归档文件夹 archive_dir 打包上传到 oss，这是自动进行的，如果因为网络原因，导致上传失败，可以通过 _archive_to_oss 方法自行上传。所以建议每次不同的清洗任务，archive_dir 路径应该设置为不同的路径，且应该确保每次清洗后的数据都能上传成功，未来任何时候都有据可依。

### 原始数据恢复

recover 方法，默认只恢复发生过更改的字段，归档文件是 change_field.txt，但 ETLBase 还提供了完整的原始数据，归档，可以通过 _recover_origin 和 _recover_changed 方法进行完整字段的恢复。

### 数据归档的格式

```python
meta = dict(
    database=database,          # 数据库
    table_name=table_name,      # 数据表
    record_id=record_id,        # 记录 ID
    field_name=field_name,      # 字段名
    origin_value=origin_value,  # 清洗前的原始值
    target_value=target_value,  # 清洗后目标值
    note=note,
)

```
### 多进程清洗

TODO：一期采用单线程处理，未来计划在 ETL 三个过程中使用多个队列，进行异步处理。
TODO：批量提交，未来做

---

参考信息：
ETLBase 基类：tests/etl/etl_base.py
ETLBase 演示：tests/etl/etl_demo.py
ETLBase 实战：tests/etl/area_district_to_city/user_area.py

表结构生成参考：https://docs.djangoproject.com/zh-hans/5.0/howto/legacy-databases/
数据库支持参考：https://docs.djangoproject.com/en/5.0/ref/settings/#std-setting-DATABASE-ENGINE
