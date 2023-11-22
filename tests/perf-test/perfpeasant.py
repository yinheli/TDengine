import os
import socket
import configparser
from runbenchmark import TaosBenchmarkRunner
from enums.DataScaleEnum import DataScaleEnum
from installtaosdb import InstallTaosDB
from util.shellutil import CommandRunner
import time
from util.templateUtil import InsertTemplate



class Peasant(object):
    def __init__(self, logger):
        # 日志信息
        self.__test_group = None
        self.__logger = logger
        self.__branch = None
        self.__data_scale = None
        # self.hostname = socket.gethostname()

        self.__commit_id = None

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

        self.__test_case_path = "test_case_{0}".format(time.time())

        self.__cmdHandler = CommandRunner(self.__logger)

    def set_branch(self, branch: str):
        self.__branch = branch

    def get_branch(self):
        return self.__branch

    def set_data_scale(self, scale: DataScaleEnum = DataScaleEnum.midweight):
        self.__data_scale = scale

    def get_data_scale(self):
        return self.__data_scale

    def set_interlace_rows(self, interlace_rows: int):
        self.__interlace_rows = interlace_rows

    def get_interlace_rows(self):
        return self.__interlace_rows

    def set_stt_trigger(self, stt_trigger: int):
        self.__stt_trigger = stt_trigger

    def get_stt_trigger(self):
        return self.__stt_trigger

    def set_test_group(self, test_group: str):
        self.__test_group = test_group

    def get_test_group(self):
        return self.__test_group

    def clean_env(self):
        self.__logger.info("【开始清理环境】")
        # 清除benchmark历史日志文件
        self.__cmdHandler.run_command(path=self.__perf_test_path, command="rm -f {0}".format("output.txt"))
        # self.__cmdHandler.run_command(path=self.__perf_test_path, command="rm -f {0}".format("output.*"))

        # self.__cmdHandler.run_command(path=self.__perf_test_path, command="rm -rf {0}".format(self.__benchmark_path))
        # self.__cmdHandler.run_command(path=self.__perf_test_path, command="mkdir -p {0}".format(self.__benchmark_path))

        # 清空log_path
        if os.path.exists(self.__log_path):
            self.__cmdHandler.run_command(path=self.__perf_test_path, command="rm -rf {0}".format(self.__log_path))
        self.__cmdHandler.run_command(path=self.__perf_test_path, command="mkdir -p {0}".format(self.__log_path))

        # tdengine_path
        # if os.path.exists(self.__tdengine_path):
        #     self.__cmdHandler.run_command(path=self.__perf_test_path, command="rm -rf {0}".format(self.__tdengine_path))
        # self.__cmdHandler.run_command(path=self.__perf_test_path, command="mkdir -p{0}".format(self.__tdengine_path))
        self.__logger.info("【完成清理环境】")

    def backup_test_case(self):
        self.__cmdHandler.run_command(path=self.__perf_test_path, command="mkdir -p {0}".format(self.__test_case_path))
        self.__cmdHandler.run_command(path=self.__perf_test_path, command="cp -r log {0}".format(self.__test_case_path))
        self.__cmdHandler.run_command(path=self.__perf_test_path, command="cp output.txt {0}".format(self.__test_case_path))

    def install_db(self):
        self.__logger.info("【开始安装TDengine】")
        # 安装db
        taosdbHandler = InstallTaosDB(logger=self.__logger)
        # 配置分支
        taosdbHandler.set_branch(branch=self.__branch)
        # 安装tdengine
        taosdbHandler.install()
        
        self.__commit_id = taosdbHandler.get_commit_id()
        self.__logger.info("【完成安装TDengine】")

    def insert_data(self):
        self.__logger.info("【开始插入数据】")
        # 执行taosBenchmark，运行性能测试用例
        # commit_id = taosdbHandler.get_commit_id()
        taosbmHandller = TaosBenchmarkRunner(logger=self.__logger)
        # 配置数据量级
        taosbmHandller.set_data_scale(data_scale=self.__data_scale)
        # 配置stt_trigger
        taosbmHandller.set_stt_trigger(stt_trigger=self.__stt_trigger)
        # 配置分支
        taosbmHandller.set_branch(branch=self.__branch)
        # 配置commit_id
        taosbmHandller.set_commit_id(commit_id=self.__commit_id)
        # 配置test_group
        taosbmHandller.set_test_group(test_group=self.__test_group)
        # 配置interlace_rows
        taosbmHandller.set_interlace_rows(interlace_rows=self.__interlace_rows)
        taosbmHandller.insert_data()

        self.__logger.info("【完成插入数据】")

    def run_test_case(self):
        self.__logger.info("【开始运行测试用例】")
        # 执行taosBenchmark，运行性能测试用例
        # commit_id = taosdbHandler.get_commit_id()
        taosbmHandller = TaosBenchmarkRunner(logger=self.__logger)
        # 配置数据量级
        taosbmHandller.set_data_scale(data_scale=self.__data_scale)
        # 配置stt_trigger
        taosbmHandller.set_stt_trigger(stt_trigger=self.__stt_trigger)
        # 配置分支
        taosbmHandller.set_branch(branch=self.__branch)
        # 配置commit_id
        taosbmHandller.set_commit_id(commit_id=self.__commit_id)
        # 配置test_group
        taosbmHandller.set_test_group(test_group=self.__test_group)
        # 配置interlace_rows
        taosbmHandller.set_interlace_rows(interlace_rows=self.__interlace_rows)
        taosbmHandller.run_test_case()

        self.__logger.info("【完成运行测试用例】")

if __name__ == "__main__":
    peasantHandler = Peasant()
    # peasantHandler.start()
        

        
        
    
