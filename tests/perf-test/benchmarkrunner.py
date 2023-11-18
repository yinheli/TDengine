import os
import configparser
from util.taosdbutil import TaosUtil
from util.templateUtil import TemplateUtil


class TaosBenchmarkRunner(object):
    def __init__(self, logger):
        # 日志信息
        self.logger = logger

        self.taosdbHandler = TaosUtil()




        # 初始化读取配置文件实例
        confile = os.path.join(os.path.dirname(__file__), "conf", "config.ini")
        self.cf = configparser.ConfigParser()
        self.cf.read(confile, encoding='UTF-8')

        self.db_install_path = self.cf.get("machineconfig", "db_install_path")


    def set_branch(self, branch: str = "main"):
        self.branch = branch
        self.logger.info("当前性能测试基于分支【" + self.branch + "】的运行...")

    def get_branch(self):
        return self.branch

    def set_data_scale(self, scale: str = "mid"):
        self.data_scale = scale

    def get_data_scale(self):
        return self.data_scale

    def get_commit_id(self):
        return self.commit_id

    def run_install_sql(self, test_cases: str):
        # 查询表test_case，获取sql等信息

        pass

    def run_query_sql(self):
        pass


if __name__ == "__main__":
    pass


        
        
    
