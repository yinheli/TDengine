import os
import configparser
from enums.DataScaleEnum import DataScaleEnum
from util.templateUtil import InsertTemplate, QueryTemplate
from util.shellutil import CommandRunner
from enums.DBDataTypeEnum import DBDataTypeEnum
from util.taosdbutil import TaosUtil


class TaosBenchmarkRunner(object):
    def __init__(self, logger):
        # 日志信息
        self.__stt_trigger = None
        self.__data_scale = None
        self.__interlace_rows = None
        self.__logger = logger

        # self.templateHandler = TemplateUtil()
        self.__branch = "main"
        self.__commit_id = None

        # 初始化读取配置文件实例
        confile = os.path.join(os.path.dirname(__file__), "conf", "config.ini")
        self.__cf = configparser.ConfigParser()
        self.__cf.read(confile, encoding='UTF-8')

        self.__db_install_path = self.__cf.get("machineconfig", "tdengine_path")
        self.__perf_test_path = self.__cf.get("machineconfig", "perf_test_path")

        self.__taosbmHandler = TaosUtil(self.__logger)

    def set_interlace_rows(self, interlace_rows: str):
        self.__interlace_rows = interlace_rows

    def get_interlace_rows(self):
        return self.__interlace_rows

    def set_data_scale(self, data_scale: DataScaleEnum):
        self.__data_scale = data_scale

    def get_data_scale(self):
        return self.__data_scale

    def set_stt_trigger(self, stt_trigger: str):
        self.__stt_trigger = stt_trigger

    def get_stt_trigger(self):
        return self.__stt_trigger

    def insert_data(self):
        insertTempHandler = InsertTemplate(data_scale=self.__data_scale, interlace_rows=self.__interlace_rows,
                                           stt_trigger=self.__stt_trigger)
        # 执行TaosBenchmark
        # cmdHandler = CommandRunner(self.__logger)
        # cmdHandler.run_command(path=self.__db_install_path, command=f"taosBenchmark -f {insertTempHandler.create_file()}")

        # 收集性能数据
        with open("/root/perftest/benchmark/insert_2023-11-21-1450.log", 'r') as f:
            # with open(insertTempHandler.get_insert_json_file(), 'r') as f:
            lines = f.readlines()
            last_2_line = lines[-2].split(' ')
            time_cost = last_2_line[4] + "s"
            write_speed = last_2_line[15] + " records/second"

            last_1_line = lines[-1].split(',')
            min = last_1_line[1].strip().split(':')[1]
            avg = last_1_line[2].strip().split(':')[1]
            p90 = last_1_line[3].strip().split(':')[1]
            p95 = last_1_line[4].strip().split(':')[1]
            p99 = last_1_line[5].strip().split(':')[1]
            max = last_1_line[6].strip().split(':')[1]

            f.close()


        # 写入数据库
        # 1.获取当前的JOB_ID
        job_id = ""
        with open("{0}/current_job_id.txt".format(self.__perf_test_path), 'r') as f:
            job_id = f.readline().strip()
            f.close()
        if job_id == "":
            self.__logger.error("获取JOB_ID失败，跳过当前任务，进入下一个job")
            return None

        # 2.判断当前branch是否有子表，没有子表的话新创建一张子表
        tag_info = [("build_branch", self.__branch, DBDataTypeEnum.string)]
        ret = self.__taosbmHandler.select(database="perf_test", table="job_details", tag_info=tag_info)
        if not ret:
            sub_table_name = "sub_table_" + self.__branch.replace(".", "_")
            self.__logger.warn("对应分支 {0} 的子表不存在，新创建子表 {1}".format(self.__branch, sub_table_name))
            self.__taosbmHandler.exec_sql(
                "create table perf_test.{0} using perf_test.job_details (build_branch, data_scale) tags ('{1}', '{2}')".format(
                    sub_table_name, self.__branch, self.__data_scale.value))
            self.__logger.info("对应分支 {0} 的子表 {1} 创建成功".format(self.__branch, sub_table_name))
        self.__logger.info("当前JOB_ID : [{0}]".format(job_id))

        # 2.往子表中插入性能数据
        # 定义表名：st_[branch]_[data_scale]
        sub_table_name = "st_{0}_{1}".format(self.__branch)
        value_info = [("commit_id", self.__commit_id, DBDataTypeEnum.string)]
        condition_info = [("jd_id", str(job_id), DBDataTypeEnum.int)]
        self.__taosbmHandler.update(database="perf_test",
                                  table="job_definition",
                                  value_info=value_info,
                                  condition_info=condition_info)

if __name__ == "__main__":
    pass
