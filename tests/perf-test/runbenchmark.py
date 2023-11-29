import os
import configparser
from util.feishuutil import FeishuUtil
from util.shellutil import CommandRunner
from util.taosdbutil import TaosUtil
import socket


class TaosBenchmarkRunner(object):
    def __init__(self, logger):
        # 日志信息
        self.__scenario_desc = None
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
        self.__feishuUtil = FeishuUtil()
    
    def set_cluster_id(self, cluster_id: int):
        self.__cluster_id = cluster_id

    def get_cluster_id(self):
        return self.__cluster_id

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

    def insert_data(self):
        # query_json_file = "tests/perf-test/test_cases/query1.json"
        if not self.__test_group:
            self.__logger.error("Test Group is not defined!")
            return

        test_group_file = os.path.join(self.__local_test_case_path, self.__test_group)
        cfHandler = configparser.ConfigParser()
        cfHandler.read(test_group_file, encoding='UTF-8')

        scenario_list = cfHandler.options(section="scenarios")

        # 轮询执行TaosBenchmark
        for scenario_file in scenario_list:
            scenario_desc = cfHandler.get(section="scenarios", option=str(scenario_file))
            self.__scenario_desc = scenario_desc

            # 执行TaosBenchmark
            cmdHandler = CommandRunner(self.__logger)
            cmdHandler.run_command(path=self.__perf_test_path, command="taosBenchmark -f {0}/{1}".format(self.__local_test_case_path, scenario_file))

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

                # 运行告警机制，若是达到告警条件，发送告警信息到feishu pot
                ret = self.__taosbmHandler.exec_sql(
                    f'select avg(write_speed) from perf_test.test_results where branch="{self.__branch}" and scenario="{self.__scenario_desc}" and test_case="{self.__scenario_desc}" and machine_info={self.__cluster_id}')
                if ret['rows'] == 1:
                    self.performance_metrics_check(current_value=write_speed, history_avg=str(ret['data'][0][0]),
                                                   test_desc=self.__scenario_desc)

            # 写入数据库
            # 定义表名：st_[cluster_id]_[branch]_[secnario_desc]_[secnario_file]
            sub_table_name = "t_{0}_{1}_{2}_{3}".format(self.__cluster_id, self.__branch, self.__scenario_desc,
                                                        scenario_file.split('.')[0]).replace('.', '_').replace('/', '_')

            base_sql = "insert into perf_test.{0} using perf_test.test_results (branch, scenario, test_case, machine_info) tags ('{1}', '{2}', '{3}',{4}) values " \
                       "(now, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, '{14}', '{15}')".format(
                sub_table_name, self.__branch, self.__scenario_desc, self.__scenario_desc, self.__cluster_id, time_cost,
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

        query_case_list = cfHandler.options(section="query_cases")

        # 轮询执行TaosBenchmark
        for query_file in query_case_list:

            query_sql = cfHandler.get(section="query_cases", option=str(query_file))
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

            # 运行告警机制，若是达到告警条件，发送告警信息到feishu pot
            ret = self.__taosbmHandler.exec_sql(
                f'select avg(avg_delay) from perf_test.test_results where branch="{self.__branch}" and scenario="{self.__scenario_desc}" and test_case="{query_sql}" and machine_info={self.__cluster_id}')
            if ret['rows'] == 1:
                self.performance_metrics_check(current_value=avg, history_avg=str(ret['data'][0][0]), test_desc=query_sql)



            # 写入数据库
            # 定义表名：st_[cluster_id]_[branch]_[scenario_desc]_[json_file_name]
            sub_table_name = "t_{0}_{1}_{2}_{3}".format(self.__cluster_id, self.__branch, self.__scenario_desc,
                                                     query_file.split('.')[0]).replace('.', '_').replace('/', '_')

            base_sql = "insert into perf_test.{0} using perf_test.test_results (branch, scenario, test_case, machine_info) tags ('{1}', '{2}', '{3}', {4}) values " \
                       "(now, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, '{14}', '{15}')".format(
                sub_table_name, self.__branch, self.__scenario_desc, query_sql, self.__cluster_id, time_cost, 0, QPS, min, p90,
                p95, p99, max, avg, socket.gethostname(), self.__commit_id)
            self.__taosbmHandler.exec_sql(base_sql)

    def performance_metrics_check(self, current_value: str, history_avg: str, test_desc: str):
        send_alarm = False
        # 当当前查询速度比历史平均速度提升达到30%以上，告警提示
        if float(current_value) <= float(history_avg) and (float(history_avg) - float(current_value)) / float(history_avg) >= 0.3:
            self.__feishuUtil.set_host("当前速度比历史平均速度提升超过30%")
            self.__logger.warning("当前速度比历史平均速度提升超过30%")
            self.__logger.warning(f"当前速度   ：{current_value}")
            self.__logger.warning(f"历史平均速度：{history_avg}")
            send_alarm = True

        # 当当前查询速度比历史平均速度降低达到10%以上，告警提示
        if float(current_value) >= float(history_avg) and (float(current_value) - float(history_avg)) / float(history_avg) >= 0.1:
            self.__feishuUtil.set_host("当前速度比历史平均速度降低超过10%")
            self.__logger.warning("当前速度比历史平均速度降低超过10%")
            self.__logger.warning(f"当前速度   ：{current_value}")
            self.__logger.warning(f"历史平均速度：{history_avg}")
            send_alarm = True

        if send_alarm:
            self.__feishuUtil.set_scenario(self.__scenario_desc)
            self.__feishuUtil.set_tc_desc(test_desc)
            self.__feishuUtil.set_branch(self.__branch)
            self.__feishuUtil.set_current_value(current_value)
            self.__feishuUtil.set_avg_history(history_avg)
            self.__feishuUtil.set_commit_id(self.__commit_id)
            self.__feishuUtil.send_msg()


if __name__ == "__main__":
    taosbmHandler = TaosUtil(None)
    ret = taosbmHandler.exec_sql(
        f'select avg(id)  from perf_test.test')
    print(ret)
    pass
