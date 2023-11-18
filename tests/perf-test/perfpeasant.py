import os
import socket
import mysqldb
import insert_json
import query_json
import buildTD
import configparser
from installtaosdb import InstallTaosDB


class PerfRunner(object):
    def __init__(self, logger):
        # 日志信息
        self.logger = logger
        self.branch = "main"
        self.data_scale = "mid"
        self.hostname = socket.gethostname()

        self.commit_id = None


        # 初始化读取配置文件实例
        confile = os.path.join(os.path.dirname(__file__), "conf", "config.ini")
        self.cf = configparser.ConfigParser()
        self.cf.read(confile, encoding='UTF-8')


    def set_branch(self, branch: str = "main"):
        self.branch = branch
        self.logger.info("当前性能测试基于分支【" + self.branch + "】的运行...")

    def get_branch(self):
        return self.branch

    def set_data_scale(self, scale: str = "mid"):
        self.data_scale = scale

    def get_data_scale(self):
        return self.data_scale

    def start(self):
        # 1.清理环境

        # 2.安装db
        taosdbHandler = InstallTaosDB()
        # 配置分支
        taosdbHandler.set_branch(branch=self.branch)
        # 配置数据量级
        taosdbHandler.set_data_scale(scale=self.data_scale)
        # 安装tdengine
        taosdbHandler.install()

        # 3.执行taosBenchmark，运行性能测试用例

        # 3.1 生成基础数据
        num_of_tables = self.cf.get(self.data_scale, "num_of_tables")
        records_per_table = self.cf.get(self.data_scale, "records_per_table")
        interlace_rows = self.cf.get(self.data_scale, "interlace_rows")
        stt_trigger = self.cf.get(self.data_scale, "stt_trigger")

        # 4.收集性能数据

        # 5.保存数据到元数据库
        pass



if __name__ == "__main__":


    # Build TDengine
    hostname = socket.gethostname()    
    new_build = buildTD.BuildTDengine(host = hostname)
        
    new_build.build()
    cmd = f"cd {new_build.path} && git rev-parse --short @ "
    commit_id = new_build.get_cmd_output(cmd)
    branch = new_build.branch
    
    num_of_tables = 10000
    records_per_table = 10000
    interlace_rows = 0
    stt_trigger = 1
    
    # get scenario id
    db = mysqldb.MySQLDatabase()
    db.connect()
    sql = f"select id from scenarios where num_of_tables = {num_of_tables} and records_per_table = {records_per_table} and interlace_rows = {interlace_rows} and stt_trigger = {stt_trigger}"
    row = db.query(sql)
    if row is None:
        id = db.get_id(f"insert into scenarios(num_of_tables, records_per_table, interlace_rows, stt_trigger) values({num_of_tables},{records_per_table}, {interlace_rows}, {stt_trigger})")
    else:
        id = row[0][0]        
    
    print(f"scenario id is {id}")
    
    # record insert performance data 
    insert = insert_json.InsertJson(num_of_tables, records_per_table, interlace_rows, stt_trigger)
    os.system(f"taosBenchmark -f {insert.create_insert_file()}")
    
    cmd = "grep Spent /tmp/insert_res.txt | tail -1 | awk {'print $5'}"
    time = new_build.get_cmd_output(cmd)
    
    cmd = "grep Spent /tmp/insert_res.txt | tail -1 | awk {'print $16'}"
    speed = new_build.get_cmd_output(cmd)
    
    sql = f"insert into insert_perf(sid, time_cost, records_per_sec, branch, commit_id, date) values({id}, {time}, {speed}, '{branch}', '{commit_id}', now())"
    print(sql)
    db.execute(sql)
    
    # record query performance data 
    sql = "select * from queries"    
    res = db.query(sql)
    for row in res:
        json = query_json.QueryJson(row[1], query_times=1)
        print(f"query: {row[1]}")
        os.system(f"taosBenchmark -f {json.create_query_file()} > /tmp/{row[0]}.txt")
        cmd = "grep delay /tmp/%d.txt | awk {'print $11'} | cut -d 's' -f 1" % row[0]
        print(f"cmd is {cmd}")
        avg = new_build.get_cmd_output(cmd)
        print(f"avg is {avg}")
        if (avg == ""):
            break
        
        sql = f"insert into query_perf(sid, qid, time_cost, branch, commit_id, date) values({id}, {row[0]}, {avg}, '{branch}', '{commit_id}', now())"
        print(sql)
        db.execute(sql)
    
    # close connection
    db.disconnect()
        

        
        
    
