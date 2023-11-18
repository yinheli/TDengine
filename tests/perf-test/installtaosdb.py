import os
import socket
import mysqldb
import insert_json
import query_json
import buildTD
import configparser
import subprocess


class InstallTaosDB(object):
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

    def install(self):
        # 1.清理环境

        # 2.安装db
        # Build & install TDengine
        parameters = [self.db_install_path, self.branch]
        build_fild = "./build.sh"
        try:
            # Run the Bash script using subprocess
            subprocess.run(['bash', build_fild] + parameters, check=True)
            print("TDengine build successfully.")
        except subprocess.CalledProcessError as e:
            print(f"Error running Bash script: {e}")
        except FileNotFoundError as e:
            print(f"File not found: {e}")

        # 查询最新的commit信息
        cmd = f"cd {self.db_install_path} && git rev-parse --short @ "
        self.commit_id = self.get_cmd_output(cmd)


    def get_cmd_output(self, cmd):
        try:
            # Run the Bash command and capture the output
            result = subprocess.run(cmd, stdout=subprocess.PIPE, shell=True, text=True)

            # Access the output from the 'result' object
            output = result.stdout

            return output.strip()
        except subprocess.CalledProcessError as e:
            print(f"Error running Bash command: {e}")



if __name__ == "__main__":
    pass


        
        
    
