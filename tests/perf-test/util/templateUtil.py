# -*- coding: utf-8 -*-
import os
import configparser
import json
import datetime
from enums.DBDataTypeEnum import DBDataTypeEnum
from enums.DataScaleEnum import DataScaleEnum

#
# class TemplateUtil(object):
#     def __init__(self):
#         pass
#
#     @property
#     def query(self):
#         return _query()
#
#     @property
#     def insert(self):
#         return _insert()

class QueryTemplate(object):
    def __init__(self, query_time: str = 1, query_sql: str = None):
        self.query_times = query_time
        self.query_sql = query_sql


    def get_cfg(self) -> dict:
        return {
            "filetype": "query",
            "cfgdir": "/etc/taos",
            "host": "127.0.0.1",
            "port": 6030,
            "user": "root",
            "password": "taosdata",
            "confirm_parameter_prompt": "no",
            "databases": "test",
            "query_times": self.query_times,
            "query_mode": "taosc",
            "specified_table_query": {
                "query_interval": 1,
                "concurrent": 1,
                "sqls": [
                    {
                        "sql": "%s" % self.query_sql,
                        "result": "./query_res.txt"
                    }
                ]
            }

        }

    def create_file(self) -> str:
        date = datetime.datetime.now()
        file_create_table = f"/tmp/query_{date:%F-%H%M}.json"

        with open(file_create_table, 'w') as f:
            json.dump(self.get_query_json(), f)

        return file_create_table

class InsertTemplate(object):
    def __init__(self,
                 data_scale: DataScaleEnum = DataScaleEnum.tinyweight,
                 interlace_rows: int = 0,
                 stt_trigger: int = 1):
        """
        初始化生成benchmark运行的数据插入json格式的类
        :param data_scale: 数据规模，目前支持大、中、小三个级别，默认为小量级：DataScaleEnum.tinyweight
        :param interlace_rows: 数据插入模式
        :param stt_trigger : 触发文件合并的落盘文件的个数
        # :return:  True/False 符合预期/不符合预期
        """

        # 初始化读取配置文件实例
        confile = os.path.join(os.path.dirname(os.path.dirname(__file__)), "conf", "config.ini")
        self.__cf = configparser.ConfigParser()
        self.__cf.read(confile, encoding='UTF-8')

        # 创建数据表的数量
        self.__num_of_tables = int(self.__cf.get(data_scale.value, "num_of_tables"))
        # 每张表中行的数量
        self.__records_per_table = int(self.__cf.get(data_scale.value, "records_per_table"))
        # 数据插入模式
        self.__interlace_rows = interlace_rows
        # 触发文件合并的落盘文件的个数
        self.__stt_trigger = stt_trigger

        self.__benchmark_path = self.__cf.get("machineconfig", "benchmark_path")

        self.time_identification = datetime.datetime.now()
        self.insert_json_file = self.__benchmark_path + f"/insert_{self.time_identification:%F-%H%M}.json"
        self.insert_log_file = self.__benchmark_path + f"/insert_{self.time_identification:%F-%H%M}.log"

    def get_insert_json_file(self):
        return self.insert_json_file

    def get_insert_log_file(self):
        return self.insert_log_file


    def __get_db_cfg(self) -> dict:
        return {
            "name": "test",
            "drop": "true",
            "replica": 1,
            "precision": "ms",
            "cachemodel": "'both'",
            "keep": 3650,
            "minRows": 100,
            "maxRows": 4096,
            "comp": 2,
            "vgroups": 10,
            "stt_trigger": self.__stt_trigger
        }

    def __get_stb_cfg(self) -> list:
        return [
            {
                "name": "meters",
                "child_table_exists": "no",
                "childtable_count": self.__num_of_tables,
                "childtable_prefix": "d",
                "escape_character": "yes",
                "auto_create_table": "yes",
                "batch_create_tbl_num": 5,
                "data_source": "rand",
                "insert_mode": "taosc",
                "non_stop_mode": "no",
                "line_protocol": "line",
                "insert_rows": self.__records_per_table,
                "childtable_limit": 10000,
                "childtable_offset": 100,
                "interlace_rows": self.__interlace_rows,
                "insert_interval": 0,
                "partial_col_num": 0,
                "disorder_ratio": 0,
                "disorder_range": 1000,
                "timestamp_step": 10,
                "start_timestamp": "2022-10-01 00:00:00.000",
                "sample_format": "csv",
                "sample_file": "./sample.csv",
                "use_sample_ts": "no",
                "tags_file": "",
                "columns": self.__get_column_list(),
                "tags": self.__get_tag_list()
            }
        ]

    def __get_column_list(self) -> list:
        return [
            {"type": "FLOAT", "name": "current", "count": 1, "max": 12, "min": 8},
            {"type": "INT", "name": "voltage", "max": 225, "min": 215},
            {"type": "FLOAT", "name": "phase", "max": 1, "min": 0},
        ]

    def __get_tag_list(self) -> list:
        return [
            {"type": "TINYINT", "name": "groupid", "max": 10, "min": 1},
            {"name": "location", "type": "BINARY", "len": 16,
             "values": ["San Francisco", "Los Angles", "San Diego", "San Jose", "Palo Alto", "Campbell",
                        "Mountain View", "Sunnyvale", "Santa Clara", "Cupertino"]}
        ]

    def get_cfg(self) -> dict:
        return {
            "filetype": "insert",
            "cfgdir": "/etc/taos",
            "host": "127.0.0.1",
            "port": 6030,
            "user": "root",
            "password": "taosdata",
            "connection_pool_size": 8,
            "thread_count": 10,
            "create_table_thread_count": 7,
            "result_file": "{0}".format(self.insert_log_file),
            "confirm_parameter_prompt": "no",
            "insert_interval": 0,
            "num_of_records_per_req": 1000,
            "max_sql_len": 1024000,
            "databases": [{
                "dbinfo": self.__get_db_cfg(),
                "super_tables": self.__get_stb_cfg()
            }]
        }

    def create_file(self) -> str:
        file_create_table = self.insert_json_file

        with open(file_create_table, 'w') as f:
            json.dump(self.get_cfg(), f)

        return file_create_table

if __name__ == "__main__":
    # tempUtil = TemplateUtil()
    # json_file = tempUtil.insert.create_file()
    # taosUtil.executeSql('show databases')
    # taosUtil.get_valid_machine()


    # value_info = [("comments", "正在清理机器环境", DBDataTypeEnum.string), ("js_desc", "Cleaning up", DBDataTypeEnum.string)]
    # condition = [("js_id", "1", DBDataTypeEnum.int)]
    # taosUtil.update(database="perf_test", table="job_status", condition_info=condition, value_info=value_info)

    # value_info = [("create_time", "now", DBDataTypeEnum.timestamp), ("comments", "正在清理机器环境", DBDataTypeEnum.string), ("js_desc", "Cleaning up", DBDataTypeEnum.string)]
    # taosUtil.insert(database="perf_test", table="job_status", value_info=value_info)
    pass