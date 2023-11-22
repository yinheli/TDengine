# -*- coding: utf-8 -*-
import requests
import os
import configparser
import json
from enums.DBDataTypeEnum import DBDataTypeEnum


class TaosUtil(object):
    def __init__(self, logger):
        self.logger = logger
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

        # self.logger.info('执行sql：{0}'.format(sql))
        ret = requests.post(self.base_url, headers=self.headers, data=sql.encode("utf-8"))
        data = json.loads(ret.text)

        if data['code'] != 0:
            self.logger.error("执行SQL：{0}".format(sql))
            self.logger.error("执行SQL失败，报错信息：{0}".format(ret.text))
        return data


    def get_valid_machine(self):
        ret = self.__executeSql("select * from perf_test.machine_info where valid = 1")
        print(ret)

    def exec_sql(self, sql):
        return self.__executeSql(sql)

    def select(self,
               database: str,
               table: str,
               target_info: list = None,
               condition_info: list = None,
               tag_info: list = None):

        if not target_info and not condition_info and not tag_info:
            base_query_sql = "select * from {0}.{1}".format(database, table)
        else:
            final_condition_str = ""
            # 拼接普通查询条件
            if condition_info:
                for condition in condition_info:
                    condition_col_name = condition[0]
                    condition_col_value = condition[1]
                    condition_col_type = condition[2]

                    final_condition_str += "{0}={1} and ".format(condition_col_name,
                                                            condition_col_value if condition_col_type == DBDataTypeEnum.int else "'{}'".format(
                                                                condition_col_value))
            # 拼接tag查询条件
            if tag_info:
                for condition in tag_info:
                    condition_tag_name = condition[0]
                    condition_tag_value = condition[1]
                    condition_tag_type = condition[2]

                    final_condition_str += "{0}={1} and ".format(condition_tag_name,
                                                            condition_tag_value if condition_tag_type == DBDataTypeEnum.int else "'{}'".format(
                                                                condition_tag_value))

            # 删除最后的多余and字符
            if condition_info or tag_info:
                final_condition_str = " where {0}".format(final_condition_str)
                final_condition_str = final_condition_str[0: -4]

            # 拼接查询返回列
            final_target_str = "*"

            if target_info:
                final_target_str = ""

                for condition in target_info:
                    final_target_str += condition + ","

                final_target_str = final_target_str[0: -1]

            base_query_sql = "select {0} from {1}.{2} {3}".format(final_target_str, database, table,
                                                                  final_condition_str)

        ret = self.__executeSql(base_query_sql)
        if ret['code'] != 0:
            return None

        return ret["data"]

    def update(self, database: str, table: str, condition_info: list, value_info: list, tag_info: list = None):
        """
        更新tdengine数据
        :param database: 数据库database名称
        :param table: 数据库表名称
        :param condition_info : 过滤数据的关键字信息，例如：condition = [("js_id", "1", DBDataTypeEnum.int)]，其中第一个元素为列名，第二个元素为值，第三个元素为列数据类型，目前仅支持int和string
        :param value_info : 需要修改的数据范围，例如：value_info = [("comments", "test", DBDataTypeEnum.string), ("js_desc", "test1", DBDataTypeEnum.string)]
        :return:  True/False 符合预期/不符合预期
        """
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

        # primary_key_value = primary_key_value if primary_key_type == DBDataTypeEnum.int else "'{}'".format(primary_key_value)
        # base_query_sql = "select create_time from {0}.{1} where {2}={3}".format(database, table, primary_key_name, primary_key_value)
        #
        # ret = self.__executeSql(base_query_sql)
        # print(ret["data"][0][0])
        # create_time = ret["data"][0][0]

        target_info = ["create_time"]
        ret = self.select(database=database, table=table, target_info=target_info, condition_info=condition_info, tag_info=tag_info)
        create_time = ret[0][0]

        # 根据ts主键，通过insert方式达到update的效果
        value_info.append(("create_time", create_time, DBDataTypeEnum.timestamp))
        self.insert(database=database, table=table, value_info=value_info, tag_info=tag_info)


    def insert(self, database: str, table: str, value_info: list, tag_info: list = None):
        """
        更新tdengine数据
        :param database: 数据库database名称
        :param table: 数据库表名称
        :param value_info : 需要修改的数据范围，例如：value_info = [("comments", "test", DBDataTypeEnum.string), ("js_desc", "test1", DBDataTypeEnum.string)]
        :param tag_info: 表的tag信息，可选参数
        # :param table_type: 表的类型，stable或table，默认为table
        :return:  True/False 符合预期/不符合预期
        """
        # 拼接数据插入部分SQL
        column_content = "("
        values_content = "("

        for value in value_info:
            column_name = value[0]
            column_value = value[1]
            column_type = value[2]

            column_content += column_name + ","
            if column_type == DBDataTypeEnum.int:
                values_content += "{},".format(column_value)
            elif column_type == DBDataTypeEnum.timestamp and column_value == "now":
                values_content += "{},".format(column_value)
            else:
                values_content += "'{}',".format(column_value)

        column_content += "update_time)"
        values_content += "now)"

        # 拼接TAG部分SQL
        tag_sql = ""
        if tag_info:
            column_content = "("
            values_content = "("

            for value in value_info:
                column_name = value[0]
                column_value = value[1]
                column_type = value[2]

                column_content += column_name + ","
                if column_type == DBDataTypeEnum.int:
                    values_content += "{},".format(column_value)
                elif column_type == DBDataTypeEnum.timestamp and column_value == "now":
                    values_content += "{},".format(column_value)
                else:
                    values_content += "'{}',".format(column_value)

            column_content = column_content[0: -1]
            values_content = values_content[0: -1]

            tag_sql = "{0} tags {1}".format(column_content, values_content)

        # e.g. insert into job_status (create_time, comments) (tc_id) tags (10006) values('2023-11-16 21:19:32.030','12345') ;
        insert_sql = "insert into {0}.{1} {2} {4} values{3}".format(database, table, column_content, values_content, tag_sql)

        ret = self.__executeSql(insert_sql)
        if ret['code'] != 0:
            self.logger.error('执行sql失败')
            return False

        return True




if __name__ == "__main__":
    taosUtil = TaosUtil()
    # taosUtil.executeSql('show databases')
    # taosUtil.get_valid_machine()


    value_info = [("comments", "正在清理机器环境", DBDataTypeEnum.string), ("js_desc", "Cleaning up", DBDataTypeEnum.string)]
    condition = [("js_id", "1", DBDataTypeEnum.int)]
    taosUtil.update(database="perf_test", table="job_status", condition_info=condition, value_info=value_info)

    # value_info = [("create_time", "now", DBDataTypeEnum.timestamp), ("comments", "正在清理机器环境", DBDataTypeEnum.string), ("js_desc", "Cleaning up", DBDataTypeEnum.string)]
    # taosUtil.insert(database="perf_test", table="job_status", value_info=value_info)