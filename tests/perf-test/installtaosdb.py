import os
import socket
import configparser
import subprocess
from enums.MetaDataEnum import JobStatusEnum
from enums.DBDataTypeEnum import DBDataTypeEnum
from util.shellutil import CommandRunner
from util.taosdbutil import TaosUtil


class InstallTaosDB(object):
    def __init__(self, logger):
        # 日志信息
        self.__version = ""
        self.__logger = logger
        self.__branch = ""
        self.__data_scale = "mid"
        self.__hostname = socket.gethostname()

        self.__commit_id = None
        self.__taosdbHandler = TaosUtil(self.__logger)

        # 初始化读取配置文件实例
        confile = os.path.join(os.path.dirname(__file__), "conf", "config.ini")
        self.__cf = configparser.ConfigParser()
        self.__cf.read(confile, encoding='UTF-8')

        self.__db_install_path = self.__cf.get("machineconfig", "tdengine_path")
        self.__perf_test_path = self.__cf.get("machineconfig", "perf_test_path")

        self.__cmdHandler = CommandRunner(self.__logger)

    def set_branch(self, branch: str = "main"):
        self.__branch = branch

    def get_branch(self):
        return self.__branch

    def set_version(self, version: str):
        self.__version = version

    def get_version(self):
        return self.__version

    # def set_data_scale(self, data_scale: DataScaleEnum):
    #     self.__data_scale = data_scale
    #
    # def get_data_scale(self):
    #     return self.__data_scale
    #
    # def set_stt_trigger(self, stt_trigger: int):
    #     self.stt_trigger = stt_trigger
    #
    # def get_stt_trigger(self):
    #     return self.stt_trigger
    #
    # def set_interlace_rows(self, interlace_rows: int):
    #     self.interlace_rows = interlace_rows
    #
    # def get_interlace_rows(self):
    #     return self.interlace_rows

    # def set_db_install_path(self, db_install_path: str = "mid"):
    #     self.__db_install_path = db_install_path
    #
    # def get_db_install_path(self):
    #     return self.__db_install_path

    def get_commit_id(self):
        stdout = self.__cmdHandler.run_command(path=self.__db_install_path, command="git rev-parse --short @")
        self.__commit_id = stdout
        return self.__commit_id

    def install(self):
        # 安装db
        try:
            # 获取最新的JOB_ID
            # job_id = 1
            # target_info = ["last(jd_id)"]
            # ret = self.__taosdbHandler.select(database="perf_test", table="job_definition", target_info=target_info)
            # if len(ret) == 1:
            #     job_id += ret[0][0]

            self.__logger.info("开始安装TDengine...")
            self.__logger.info("代码Branch : [{0}]".format(self.__branch))
            # self.__logger.info("当前Version：{0}".format(self.__branch))
            cluster_id = "1"
            condition_info = [("cluster_id", cluster_id, DBDataTypeEnum.int), ("valid", "1", DBDataTypeEnum.int)]
            machine_info = self.__taosdbHandler.select(database="perf_test", table="machine_info",
                                                       condition_info=condition_info)

            self.__logger.info("执行机器信息 ->")
            self.__logger.info("[")
            for machine in machine_info:
                self.__logger.info("  HostIP  : {0}".format(machine[2]))
                self.__logger.info("  Leader  : {0}".format(machine[3]))
                self.__logger.info("  CPU     : {0}".format(machine[5]))
                self.__logger.info("  MEM     : {0}".format(machine[6]))
                self.__logger.info("  DISK    : {0}".format(machine[7]))
                self.__logger.info("")
            self.__logger.info("]")

            # value_info = [("create_time", "now", DBDataTypeEnum.timestamp),
            #               ("jd_id", str(job_id), DBDataTypeEnum.int),
            #               ("js_id", JobStatusEnum.Installing.value, DBDataTypeEnum.int),
            #               ("cluster_id", cluster_id, DBDataTypeEnum.int),
            #               ("branch", self.__branch, DBDataTypeEnum.string)]
            # # ("pv_id", self.__version, DBDataTypeEnum.int)]
            # self.__taosdbHandler.insert(database="perf_test", table="job_definition", value_info=value_info)
            # self.__logger.info("当前JOB_ID : [{0}]".format(job_id))

            self.__cmdHandler = CommandRunner(self.__logger)
            self.__cmdHandler.run_command(path=self.__db_install_path, command="git reset --hard HEAD")
            self.__cmdHandler.run_command(path=self.__db_install_path, command="git checkout -- .")
            self.__cmdHandler.run_command(path=self.__db_install_path, command="git checkout {0}".format(self.__branch))

            # 查询最新的commit信息
            # stdout = self.__cmdHandler.run_command(path=self.__db_install_path, command="git rev-parse --short @")
            # self.__commit_id = stdout

            # value_info = [("commit_id", self.__commit_id, DBDataTypeEnum.string)]
            # condition_info = [("jd_id", str(job_id), DBDataTypeEnum.int)]
            # self.__taosdbHandler.update(database="perf_test",
            #                             table="job_definition",
            #                             value_info=value_info,
            #                             condition_info=condition_info)

            # 保存JOB_ID到本地文件，后续步骤读取使用
            # with open("{0}/current_job_id.txt".format(self.__perf_test_path), 'w') as f:
            #     f.write(str(job_id))
            #     f.close()

            self.__cmdHandler.run_command(path=self.__db_install_path, command="git pull")
            self.__cmdHandler.run_command(path=self.__db_install_path,
                                   command="sed -i ':a;N;$!ba;s/\(.*\)OFF/\\1ON/' {0}/cmake/cmake.options".format(
                                       self.__db_install_path))
            self.__cmdHandler.run_command(path=self.__db_install_path, command="mkdir -p {0}/debug".format(self.__db_install_path))
            self.__cmdHandler.run_command(path=self.__db_install_path, command="rm -rf {0}/debug/*".format(self.__db_install_path))
            self.__cmdHandler.run_command(path=self.__db_install_path + "/debug", command="cmake .. -DBUILD_TOOLS=true")
            self.__cmdHandler.run_command(path=self.__db_install_path + "/debug", command="make -j 4")
            self.__cmdHandler.run_command(path=self.__db_install_path + "/debug", command="make install")
            self.__cmdHandler.run_command(path=self.__db_install_path + "/debug", command="systemctl start taosd")

            self.__logger.info("TDengine start successfully.")
        except Exception as e:
            self.__logger.error(f"Error running Bash script: {e}")


if __name__ == "__main__":
    pass
