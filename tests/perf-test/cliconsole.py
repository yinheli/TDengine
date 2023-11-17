# -*- coding: utf-8 -*-
import os
import sys
import logging
import traceback
import coloredlogs
import click
import configparser
from util.githubutil import GitHubUtil
from perfpeasant import PerfRunner

# 初始化日志处理
def initLogger(loggerName: str):
    # 开启日志记录
    logger = logging.getLogger(loggerName)

    # 设置程序的日志级别, 并按照coloredlogs的要求标记各种图形化颜色显示
    os.environ["COLOREDLOGS_LEVEL_STYLES"] = \
        "spam=22;debug=28;verbose=34;notice=220;warning=202;success=118,bold;" \
        "error=background=red,bold;critical=background=red"
    LOG_FORMAT = "%(asctime)s - %(levelname)9s - %(message)s"
    logFormat = logging.Formatter(LOG_FORMAT)
    consoleLogHandler = logging.StreamHandler()
    consoleLogHandler.setFormatter(logFormat)

    # 默认日志输出级别是INFO级别
    consoleLogHandler.setLevel(logging.INFO)
    coloredlogs.install(
        level=consoleLogHandler.level,
        fmt=LOG_FORMAT,
        logger=logger,
        isatty=True
    )
    return logger


@click.group(invoke_without_command=True)
@click.pass_context
@click.option("--version", is_flag=True, help="Show Farm version.")
def cli(ctx, version):
    if not ctx.invoked_subcommand:
        # 打印版本信息
        if version:
            import pkg_resources
            try:
                click.secho("Version: " + pkg_resources.get_distribution("farm").version)
            except pkg_resources.DistributionNotFound:
                click.secho("Version: 1.0.0.0")
            return
        click.echo(ctx.get_help())

# @cli.command(help="基于用户指定版本运行性能测试")
# @click.option("--version", "-v", type=str, required=True, help="指定的产品版本号，用逗号分隔.", )
# @click.option("--logfile", "-l", type=str, help="指定文件的日志位置.", )
# def run_PerfTest(
#         branch,
#         logfile
# ):
#     pass

# @cli.command(help="启动一个后台服务，在服务中根据用户输入的分支信息，循环执行性能测试")
# @click.option("--branches", "-b", type=str, required=True, help="指定分支信息，用逗号分隔.", )
# @click.option("--data-scale", "-s", type=str, required=False, default="mid", help="性能测试数据规模，暂支持3个级别：big、mid和tiny.", )
def run_PerfTest_Backend(
        branches,
        data_scale
):
    # 初始化配置文件读取实例
    confile = os.path.join(os.path.dirname(__file__), "conf", "config.ini")
    cf = configparser.ConfigParser()
    cf.read(confile, encoding='UTF-8')

    # 获取github上对应repo的所有分支
    github_repo = "{0}/{1}".format(cf.get("github", "namespace"), cf.get("github", "project"))
    github = GitHubUtil(github_repo)
    # current_branches = github.get_last_commit_by_branch(branch='3.0')
    current_branches = github.get_branches()

    # 解析branch参数
    # 参数校验，若输入参数中有不存在的分支，直接退出
    branch_list = branches.split(',')
    for branch in branch_list:
        if branch not in current_branches:
            print('分支{0}不存在'.format(branch))
            exit(1)

    # 循环执行性能测试
    while True:
        # 轮询每个配置的分支，运行一次性能测试
        for branch in branch_list:
            # 初始化性能执行器
            appLogger = initLogger("Performance_testing")
            perftestHandler = PerfRunner(logger=appLogger)

            # 配置分支
            perftestHandler.set_branch(branch=branch)
            # 配置数据量级
            perftestHandler.set_data_scale(scale=data_scale)
            # 开始跑起来
            perftestHandler.run()





@cli.command(help="关闭后台运行性能测试的服务，会确保正在运行的测试完成后才会停止服务")
@click.option("--wait-for-finished", type=str, default=True, help="是否等待正在运行的性能测试完成后在停止服务.默认为True", )
def abort_PerfTest_Backend(
        wait_for_finished
):
    print(wait_for_finished)
    pass



if __name__ == "__main__":

    run_PerfTest_Backend("main,3.0,3.1")
    # 禁用paramiko的一些不必要日志
    logging.getLogger("paramiko").setLevel(logging.WARNING)

    # 运行应用程序
    try:
        cli()
        sys.exit(0)
    except Exception as ge:
        print('traceback.print_exc():\n%s' % traceback.print_exc())
        print('traceback.format_exc():\n%s' % traceback.format_exc())
        click.secho(repr(ge), err=True, fg="red")
        sys.exit(1)
