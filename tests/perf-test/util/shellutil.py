import subprocess


class CommandRunner(object):
    def __init__(self, logger):
        # 日志信息
        self.__logger = logger

    def run_command(self, path: str, command: str):
        try:
            # Run the Bash script using subprocess
            self.__logger.info("路径 [{1}] 下执行命令：'{0}'".format(command, path))
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


        
        
    
