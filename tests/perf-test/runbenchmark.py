import os
import configparser
from enums.DataScaleEnum import DataScaleEnum
from util.shellutil import CommandRunner
from util.taosdbutil import TaosUtil
import socket


class TaosBenchmarkRunner(object):
    def __init__(self, logger):
        # 日志信息
        self.__cluster_id = None
        self.__test_group = None
        self.__stt_trigger = None
        self.__data_scale = None
        self.__interlace_rows = None
        self.__logger = logger

        # self.templateHandler = TemplateUtil()
        self.__branch = "main"
        self.__commit_id = ""

        # 初始化读取配置文件实例
        confile = os.path.join(os.path.dirname(__file__), "conf", "config.ini")
        self.__cf = configparser.ConfigParser()
        self.__cf.read(confile, encoding='UTF-8')

        self.__db_install_path = self.__cf.get("machineconfig", "tdengine_path")
        self.__perf_test_path = self.__cf.get("machineconfig", "perf_test_path")
        self.__log_path = self.__cf.get("machineconfig", "log_path")
        self.__test_case_path = self.__cf.get("machineconfig", "test_case_path")

        self.__taosbmHandler = TaosUtil(self.__logger)

        self.__cmdHandler = CommandRunner(self.__logger)

        self.__local_test_case_path = os.path.join(os.path.dirname(__file__), "test_cases")

    def set_interlace_rows(self, interlace_rows: str):
        self.__interlace_rows = interlace_rows

    def get_interlace_rows(self):
        return self.__interlace_rows
    
    def set_cluster_id(self, cluster_id: int):
        self.__cluster_id = cluster_id

    def get_cluster_id(self):
        return self.__cluster_id

    def set_data_scale(self, data_scale: DataScaleEnum):
        self.__data_scale = data_scale

    def get_data_scale(self):
        return self.__data_scale

    def set_branch(self, branch: str):
        self.__branch = branch

    def get_branch(self):
        return self.__branch

    def set_test_group(self, test_group: str):
        self.__test_group = test_group

    def get_test_group(self):
        return self.__test_group

    def set_commit_id(self, commit_id: str):
        self.__commit_id = commit_id

    def get_commit_id(self):
        return self.__commit_id

    def set_stt_trigger(self, stt_trigger: str):
        self.__stt_trigger = stt_trigger

    def get_stt_trigger(self):
        return self.__stt_trigger

    def insert_data(self):
        # query_json_file = "tests/perf-test/test_cases/query1.json"
        if not self.__test_group:
            self.__logger.error("Test Group is not defined!")
            return

        test_group_file = os.path.join(self.__local_test_case_path, self.__test_group)
        cfHandler = configparser.ConfigParser()
        cfHandler.read(test_group_file, encoding='UTF-8')

        insert_case_list = cfHandler.options(section="insert_case")

        # 轮询执行TaosBenchmark
        for insert_file in insert_case_list:
            insert_desc = cfHandler.get(section="insert_case", option=str(insert_file))

            # 执行TaosBenchmark
            cmdHandler = CommandRunner(self.__logger)
            cmdHandler.run_command(path=self.__perf_test_path, command="taosBenchmark -f {0}/{1}".format(self.__local_test_case_path, insert_file))

            # 收集性能数据
            with open("{0}/insert_result.txt".format(self.__log_path), 'r') as f:
                # with open(insertTempHandler.get_insert_json_file(), 'r') as f:
                lines = f.readlines()
                last_2_line = lines[-2].split(' ')
                time_cost = last_2_line[4]
                write_speed = last_2_line[15]

                last_1_line = lines[-1].split(',')
                min = last_1_line[1].strip().split(':')[1].strip()[0: -2]
                avg = last_1_line[2].strip().split(':')[1].strip()[0: -2]
                p90 = last_1_line[3].strip().split(':')[1].strip()[0: -2]
                p95 = last_1_line[4].strip().split(':')[1].strip()[0: -2]
                p99 = last_1_line[5].strip().split(':')[1].strip()[0: -2]
                max = last_1_line[6].strip().split(':')[1].strip()[0: -2]

                f.close()

            # 写入数据库
            # 定义表名：st_[cluster_id]_[branch]_[data_scale]
            sub_table_name = "st_{0}_{1}_{2}_{3}".format(self.__cluster_id, self.__branch, self.__data_scale.value,
                                                         insert_file.split('.')[0]).replace('.', '_')

            base_sql = "insert into perf_test.{0} using perf_test.test_results (branch, data_scale, tc_desc,machine_info) tags ('{1}', '{2}', '{3}',{4}) values " \
                       "(now, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, '{14}', '{15}')".format(
                sub_table_name, self.__branch, self.__data_scale.value, insert_desc, self.__cluster_id, time_cost,
                write_speed, 0, min, p90,
                p95, p99, max, avg, socket.gethostname(), self.__commit_id)
            self.__taosbmHandler.exec_sql(base_sql)

    def run_test_case(self):
        # query_json_file = "tests/perf-test/test_cases/query1.json"
        if not self.__test_group:
            self.__logger.error("Test Group is not defined!")
            return

        test_group_file = os.path.join(self.__local_test_case_path, self.__test_group)
        cfHandler = configparser.ConfigParser()
        cfHandler.read(test_group_file, encoding='UTF-8')

        query_case_list = cfHandler.options(section="query_case")

        # 轮询执行TaosBenchmark
        for query_file in query_case_list:

            query_sql = cfHandler.get(section="query_case", option=str(query_file))
            self.__cmdHandler.run_command(path=self.__perf_test_path,
                                          command="taosBenchmark -f {0}/{1}".format(self.__local_test_case_path,
                                                                                    query_file))

            # 收集性能数据
            with open("{0}/output.txt".format(self.__perf_test_path), 'r') as f:
                lines = f.readlines()
                last_1_line = lines[-3].split(' ')
                min = last_1_line[12].strip()[0: -1]
                avg = last_1_line[10].strip()[0: -1]
                p90 = last_1_line[16].strip()[0: -1]
                p95 = last_1_line[18].strip()[0: -1]
                p99 = last_1_line[20].strip()[0: -1]
                max = last_1_line[14].strip()[0: -1]

                last_2_line = lines[-2].split(' ')
                time_cost = last_2_line[4].strip()
                QPS = last_2_line[-1].strip()
                f.close()

            # 写入数据库
            # 定义表名：st_[cluster_id]_[branch]_[data_scale]_[json_file_name]
            sub_table_name = "st_{0}_{1}_{2}_{3}".format(self.__cluster_id, self.__branch, self.__data_scale.value,
                                                     query_file.split('.')[0]).replace('.', '_')

            base_sql = "insert into perf_test.{0} using perf_test.test_results (branch, data_scale, tc_desc, machine_info) tags ('{1}', '{2}', '{3}', {4}) values " \
                       "(now, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, '{14}', '{15}')".format(
                sub_table_name, self.__branch, self.__data_scale.value, query_sql, self.__cluster_id, time_cost, 0, QPS, min, p90,
                p95, p99, max, avg, socket.gethostname(), self.__commit_id)
            self.__taosbmHandler.exec_sql(base_sql)
if __name__ == "__main__":
    pass
