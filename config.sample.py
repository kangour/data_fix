# 配置文件


class DBConfig:
    """
    数据库配置
    """
    host = "xxx"
    port = 3306
    database = "xxx"
    username = "xxx"
    password = "xxx"
    database_engine = "mysql"  # 数据库引擎，支持 mysql, postgresql, oracle, sqlite3


class OssConfig:
    """
    阿里云 OSS 服务相关的配置文件
    """

    auto_upload_oss = False  # 自动打包上传到 oss
    access_id = "xxx"
    access_key = "xxx"
    # oss_endpoint = "http://oss-cn-hangzhou-internal.aliyuncs.com"
    oss_endpoint = "http://oss-cn-hangzhou.aliyuncs.com"
    oss_bucket_name = "xxx"
