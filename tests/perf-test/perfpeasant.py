import os
import socket
import configparser
from runbenchmark import TaosBenchmarkRunner
from enums.DataScaleEnum import DataScaleEnum
from installtaosdb import InstallTaosDB
from util.templateUtil import InsertTemplate



class Peasant(object):
    def __init__(self, logger):
        # 日志信息
        self.logger = logger
        self.branch = "main"
        self.data_scale = DataScaleEnum.midweight
        self.hostname = socket.gethostname()

        self.commit_id = None

        self.stt_trigger = None
        self.interlace_rows = None

        # 初始化读取配置文件实例
        confile = os.path.join(os.path.dirname(__file__), "conf", "config.ini")
        self.__cf = configparser.ConfigParser()
        self.__cf.read(confile, encoding='UTF-8')

        self.perf_test_path = self.__cf.get("machineconfig", "perf_test_path")
        self.benchmark_path = self.__cf.get("machineconfig", "benchmark_path")
        self.tdengine_path = self.__cf.get("machineconfig", "tdengine_path")
        self.history_path = self.__cf.get("machineconfig", "history_path")

    def set_branch(self, branch: str = "main"):
        self.branch = branch

    def get_branch(self):
        return self.branch

    def set_data_scale(self, scale: DataScaleEnum = DataScaleEnum.midweight):
        self.data_scale = scale

    def get_data_scale(self):
        return self.data_scale

    def set_interlace_rows(self, interlace_rows: int):
        self.interlace_rows = interlace_rows

    def get_interlace_rows(self):
        return self.interlace_rows

    def set_stt_trigger(self, stt_trigger: int):
        self.stt_trigger = stt_trigger

    def get_stt_trigger(self):
        return self.stt_trigger

    def clean_env(self):
        # todo 清理环境
        pass

    def install_db(self):
        # 安装db
        taosdbHandler = InstallTaosDB(logger=self.logger)
        # 配置分支
        taosdbHandler.set_branch(branch=self.branch)
        # 安装tdengine
        taosdbHandler.install()

    def insert_data(self):
        # todo 这里需要加循环执行的逻辑，计算插入和查询的平均值

        # 3.执行taosBenchmark，运行性能测试用例
        # commit_id = taosdbHandler.get_commit_id()
        taosbmHandller = TaosBenchmarkRunner(logger=self.logger)
        # 配置数据量级
        taosbmHandller.set_data_scale(data_scale=self.data_scale)
        # 配置stt_trigger
        taosbmHandller.set_stt_trigger(stt_trigger=self.stt_trigger)
        # 配置interlace_rows
        taosbmHandller.set_interlace_rows(interlace_rows=self.interlace_rows)
        taosbmHandller.insert_data()


        # 4.收集性能数据

        # 5.保存数据到元数据库

        # 6.备份linux测试机上的测试文件

    def run_test_case(self):
        # todo 清理环境
        pass


if __name__ == "__main__":
    peasantHandler = Peasant()
    peasantHandler.start()
        

        
        
    
