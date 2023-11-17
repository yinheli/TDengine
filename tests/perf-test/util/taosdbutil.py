# -*- coding: utf-8 -*-
import requests
import os
import configparser
import json
from enums.DBDataTypeEnum import DBDataTypeEnum


class TaosUtil(object):
    def __init__(self):
        self.user = None
        self.password = None
        self.database = None
        self.host = None
        self.rest_port = None
        self.port = None
        self.base_url = None

        # 读取配置文件
        confile = os.path.join(os.path.dirname(os.path.dirname(__file__)), "conf", "config.ini")
        cf = configparser.ConfigParser()
        cf.read(confile, encoding='UTF-8')

        # 读取元数据库链接信息
        self.host = cf.get("metadata", "host")
        self.port = cf.get("metadata", "port")
        self.user = cf.get("metadata", "user")
        self.password = cf.get("metadata", "passwd")
        self.ldapServer = cf.get("metadata", "database")
        self.rest_port = cf.get("metadata", "rest_port")
        self.rest_token = cf.get("metadata", "rest_token")

        self.base_url = "http://{0}:{1}/rest/sql".format(self.host, self.rest_port)
        self.headers = {"Authorization": "Basic cm9vdDp0YW9zZGF0YQ==", "Accept-Charset": "utf-8"}


    def __executeSql(self, sql=None):
        if sql is None:
            return None

        ret = requests.post(self.base_url, headers=self.headers, data=sql.encode("utf-8"))
        data = json.loads(ret.text)

        if data['code'] != 0:
            print(ret.text)

        return data


    def get_valid_machine(self):
        ret = self.__executeSql("select * from perf_test.machine_info where valid = 1")
        print(ret)


    def update(self, database: str, table: str, condition_info: list, value_info: list):
        """
        更新tdengine数据
        :param database: 数据库database名称
        :param table: 数据库表名称
        :param condition_info : 过滤数据的关键字信息，例如：condition = [("js_id", "1", DBDataTypeEnum.int)]，其中第一个元素为列名，第二个元素为值，第三个元素为列数据类型，目前仅支持int和string
        :param value_info : 需要修改的数据范围，例如：value_info = [("comments", "test", DBDataTypeEnum.string), ("js_desc", "test1", DBDataTypeEnum.string)]
        :return:  True/False 符合预期/不符合预期
        """
        create_time = None
        # 获取primary_key信息
        primary_key_name = None
        primary_key_value = None
        primary_key_type = None

        for condition in condition_info:
            primary_key_name = condition[0]
            primary_key_value = condition[1]
            primary_key_type = condition[2]


        # 查询到需要修改行的ts
        if primary_key_value is None or primary_key_name is None:
            print("primary key is invalid")
        # primary_key_value = "sdfds"

        primary_key_value = primary_key_value if primary_key_type == DBDataTypeEnum.int else "'{}'".format(primary_key_value)
        base_query_sql = "select create_time from {0}.{1} where {2}={3}".format(database, table, primary_key_name, primary_key_value)

        ret = self.__executeSql(base_query_sql)
        print(ret["data"][0][0])
        create_time = ret["data"][0][0]

        # 根据ts主键，通过insert方式达到update的效果
        column_content = "(create_time,"
        values_content = "('{}',".format(create_time)

        for value in value_info:
            column_name = value[0]
            column_value = value[1]
            column_type = value[2]

            column_content += column_name + ","
            values_content += column_value if column_type == DBDataTypeEnum.int else "'{}',".format(column_value)

        column_content += "update_time)"
        values_content += "now)"

        # e.g. insert into job_status (create_time, comments) values('2023-11-16 21:19:32.030','12345') ;
        insert_sql = "insert into {0}.{1} {2} values{3}".format(database, table, column_content, values_content)
        ret = self.__executeSql(insert_sql)
        if ret['code'] != 0:
            print('执行sql失败')
            return 1

        return 0





    # def insert(self, sql=None):
    #     if sql is None:
    #         return None
    #
    #     ret = requests.post('http://192.168.1.204:6041/rest/sql', headers=self.headers, data=sql)
    #     data = json.loads(ret)
    #     print(ret)
    #
    #
    # def update(self, sql=None):
    #     if sql is None:
    #         return None
    #
    #     ret = requests.post('http://192.168.1.204:6041/rest/sql', headers=self.headers, data=sql)
    #     print(ret)

if __name__ == "__main__":
    taosUtil = TaosUtil()
    # taosUtil.executeSql('show databases')
    # taosUtil.get_valid_machine()
    value_info = [("comments", "正在清理机器环境", DBDataTypeEnum.string), ("js_desc", "Cleaning up", DBDataTypeEnum.string)]
    condition = [("js_id", "1", DBDataTypeEnum.int)]
    taosUtil.update(database="perf_test", table="job_status", condition_info=condition, value_info=value_info)