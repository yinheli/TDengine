import subprocess


class CommandRunner(object):
    def __init__(self, logger):
        # 日志信息
        self.__logger = logger

        # 初始化读取配置文件实例
        # confile = os.path.join(os.path.dirname(__file__), "conf", "config.ini")
        # self.cf = configparser.ConfigParser()
        # self.cf.read(confile, encoding='UTF-8')
        #
        # self.db_install_path = self.cf.get("machineconfig", "tdengine_path")

    def run_command(self, path: str, command: str):
        try:
            # Run the Bash script using subprocess
            self.__logger.info("执行命令：'{0}'".format(command))
            # result = subprocess.run(command, check=True,  cwd=path, stdout=subprocess.PIPE)
            result = subprocess.run(command, check=True, shell=True, cwd=path, stdout=subprocess.PIPE)

            if result.returncode == 0:
                self.__logger.info("执行成功，返回信息：{0}".format(result.stdout.decode('utf-8').strip()))
                return result.stdout.decode('utf-8').strip()
            else:
                self.__logger.error("执行失败，返回信息：{0}".format(result.stderr.decode('utf-8').strip()))
                return result.stderr.decode('utf-8').strip()

        except subprocess.CalledProcessError as e:
            self.__logger.error(f"Error running Bash script: {e}")
        except FileNotFoundError as e:
            self.__logger.error(f"File not found: {e}")



if __name__ == "__main__":
    pass


        
        
    
