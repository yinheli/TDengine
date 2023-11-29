import os
import time
import configparser
from runbenchmark import TaosBenchmarkRunner
from enums.DataScaleEnum import DataScaleEnum
from installtaosdb import InstallTaosDB
from util.shellutil import CommandRunner
import time
from util.taosdbutil import TaosUtil


class Peasant(object):
    def __init__(self, logger):
        # 日志信息
        self.__machine_info = None
        self.__cluster_id = None
        self.__test_group = None
        self.__logger = logger
        self.__branch = None
        self.__data_scale = None

        self.__commit_id = ""

        self.__stt_trigger = None
        self.__interlace_rows = None

        # 初始化读取配置文件实例
        confile = os.path.join(os.path.dirname(__file__), "conf", "config.ini")
        self.__cf = configparser.ConfigParser()
        self.__cf.read(confile, encoding='UTF-8')

        self.__perf_test_path = self.__cf.get("machineconfig", "perf_test_path")
        self.__benchmark_path = self.__cf.get("machineconfig", "benchmark_path")
        self.__tdengine_path = self.__cf.get("machineconfig", "tdengine_path")
        self.__log_path = self.__cf.get("machineconfig", "log_path")
        self.__db_clone_address = self.__cf.get("github", "db_clone_address")
        self.__local_test_case_path = os.path.join(os.path.dirname(__file__), "test_cases")

        self.__test_case_path = "test_case_{0}".format(time.time())

        self.__cmdHandler = CommandRunner(self.__logger)

        self.__taosbmHandler = TaosUtil(self.__logger)

        self.__taosdbHandler = InstallTaosDB(logger=self.__logger)

        self.__taosbmHandller = TaosBenchmarkRunner(logger=self.__logger)

    def set_branch(self, branch: str):
        self.__branch = branch

    def get_branch(self):
        return self.__branch

    def set_machine(self, cluster_id: str):
        self.__cluster_id = cluster_id

    def get_machine(self):
        return self.__cluster_id


    def set_test_group(self, test_group: str):
        self.__test_group = test_group

    def get_test_group(self):
        return self.__test_group

    def get_commit_id(self):
        return self.__commit_id

    def clean_env(self):
        self.__logger.info("【初始化环境】")

        if not os.path.exists(self.__tdengine_path):
            self.__logger.info("初始化工作目录")
            self.__cmdHandler.run_command(path=self.__perf_test_path,
                                          command="mkdir -p {0}".format(self.__tdengine_path))

        # 清除benchmark历史日志文件
        self.__logger.info("清除benchmark历史日志文件")
        self.__cmdHandler.run_command(path=self.__perf_test_path, command="rm -f {0}".format("output.txt"))

        # 清空log_path
        self.__logger.info("清空log_path")
        if os.path.exists(self.__log_path):
            self.__cmdHandler.run_command(path=self.__perf_test_path, command="rm -rf {0}".format(self.__log_path))
        self.__cmdHandler.run_command(path=self.__perf_test_path, command="mkdir -p {0}".format(self.__log_path))

        # 清除taosd本地残留文件
        self.__logger.info("清除taosd本地残留文件")
        self.__cmdHandler.run_command(path=self.__perf_test_path, command="rm -rf /etc/taos")
        self.__cmdHandler.run_command(path=self.__perf_test_path, command="rm -rf /var/lib/taos")
        self.__cmdHandler.run_command(path=self.__perf_test_path, command="rm -rf /var/log/taos")

        self.__logger.info("【完成初始化环境】")

    def backup_test_case(self):
        self.__logger.info("【开始备份数据】")
        self.__logger.info("数据路目录: [{0}]".format(self.__test_case_path))
        self.__cmdHandler.run_command(path=self.__perf_test_path, command="mkdir -p {0}".format(self.__test_case_path))
        self.__cmdHandler.run_command(path=self.__perf_test_path,
                                      command="cp log/insert_result.txt {0}".format(self.__test_case_path))
        self.__cmdHandler.run_command(path=self.__perf_test_path,
                                      command="cp output.txt {0}".format(self.__test_case_path))
        self.__logger.info("【完成备份数据】")

    def install_db(self):
        self.__logger.info("【安装TDengine】")

        # 配置分支
        self.__taosdbHandler.set_branch(branch=self.__branch)
        # 配置cluster_id
        self.__taosdbHandler.set_cluster_id(cluster_id=self.__cluster_id)
        # 安装tdengine
        self.__taosdbHandler.install()

        self.__commit_id = self.__taosdbHandler.get_commit_id()
        time.sleep(5)
        self.__logger.info("【完成安装TDengine】")

    def download_db(self):
        self.__logger.info("【下载TDengine源代码】")
        # 配置分支
        self.__taosdbHandler.set_branch(branch=self.__branch)
        # 配置cluster_id
        self.__taosdbHandler.set_cluster_id(cluster_id=self.__cluster_id)
        # 安装tdengine
        self.__taosdbHandler.download()

        self.__commit_id = self.__taosdbHandler.get_commit_id()
        self.__logger.info("【完成下载TDengine源代码】")

    def insert_data(self):
        self.__logger.info("【插入数据】")
        # 执行taosBenchmark，运行性能测试用例
        # 配置cluster_id
        self.__taosbmHandller.set_cluster_id(cluster_id=self.__cluster_id)
        # 配置分支
        self.__taosbmHandller.set_branch(branch=self.__branch)
        # 配置commit_id
        self.__taosbmHandller.set_commit_id(commit_id=self.__commit_id)
        # 配置test_group
        self.__taosbmHandller.set_test_group(test_group=self.__test_group)

        self.__taosbmHandller.insert_data()

        self.__logger.info("【完成插入数据】")

    def exec_test(self):
        self.__logger.info("【运行测试用例】")
        # 执行taosBenchmark，运行性能测试用例
        # 配置cluster_id
        self.__taosbmHandller.set_cluster_id(cluster_id=self.__cluster_id)
        # 配置分支
        self.__taosbmHandller.set_branch(branch=self.__branch)
        # 配置commit_id
        self.__taosbmHandller.set_commit_id(commit_id=self.__commit_id)
        # 配置test_group
        # self.__taosbmHandller.set_test_group(test_group=self.__test_group)

        test_group_file = os.path.join(self.__local_test_case_path, self.__test_group)
        cfHandler = configparser.ConfigParser()
        cfHandler.read(test_group_file, encoding='UTF-8')

        scenario_list = cfHandler.options(section="scenarios")

        # 轮询执行TaosBenchmark
        # 轮询执行scenario
        for scenario_file in scenario_list:
            scenario_desc = cfHandler.get(section="scenarios", option=str(scenario_file))
            scenario_info = {'scenario_file': scenario_file, 'scenario_desc': scenario_desc}
            self.__taosbmHandller.insert_data(scenario_info=scenario_info)

            # 每个scenario下轮询执行test case
            query_case_list = cfHandler.options(section="query_cases")

            for query_file in query_case_list:
                query_sql = cfHandler.get(section="query_cases", option=str(query_file))
                tc_info = {'query_file': query_file, 'query_sql': query_sql}
                self.__taosbmHandller.run_test_case(tc_info=tc_info)

        self.__logger.info("【完成运行测试用例】")

    def is_last_commit(self, branch: str):
        self.__logger.info("判断当前分支的commit ID是否已有性能数据，若有的话，不在重复执行性能测试用例")
        base_sql = "select last(commit_id) from perf_test.test_results where branch = '{0}'".format(branch)
        ret = self.__taosbmHandler.exec_sql(base_sql)
        if ret['rows'] == 0:
            self.__logger.info("当前分支没有性能测试数据，直接运行性能测试")
            return False

        last_commit_id = ret['data'][0][0]

        # 获取当前分支最新的commit_id
        git_commit_id = self.__cmdHandler.run_command(path=self.__tdengine_path, command="git rev-parse --short @")

        self.__logger.info(
            "当前分支 [{0}]的最新commit_id为 [{1}]，性能数据历史最新的commit_id为 [{2}]".format(branch, git_commit_id, last_commit_id))
        if git_commit_id == last_commit_id:
            return True

        return False


if __name__ == "__main__":
    peasantHandler = Peasant()
    # peasantHandler.start()
