###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import taos
from taos.tmq import *
from util.cases import *
from util.common import *
from util.log import *
from util.sql import *
from util.sqlset import *
import threading

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.setsql = TDSetSql()
        self.dbname = 'db'
        self.stbname = 'stb'
        self.user = 'chr'
        self.passwd = 'test'
        self.binary_length = 20  # the length of binary for column_dict
        self.nchar_length = 20  # the length of nchar for column_dict

    def prepare_data(self):
        tdSql.execute(f"create database {self.dbname}")


    def create_user(self):
        tdSql.execute(f'create user {self.user} pass "test " ')

    def run_command_failed(self, shell_cmd):
        # tdLog.info(f"run command: {command}")
        result = os.popen(shell_cmd).read()
        print("failed ?")
        print(result)
        if ("\"code\":0," in result):
            tdLog.exit(f"command:{shell_cmd}, expect error not  occured")
        else:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            tdLog.debug(f"{caller.filename}({caller.lineno}) failed: command:{shell_cmd}, expect error  occured")
        pass

    def run_command_success(self, shell_cmd):
        # tdLog.info(f"run command: {command}")
        result = os.popen(shell_cmd).read()
        print("success ?")
        print(result)

        if ("\"code\":0," in result):
            tdLog.info(f"command:{shell_cmd}, expect successful executed")
        else:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            tdLog.exit(f"{caller.filename}({caller.lineno}) failed: command:{shell_cmd},   failed executed")

    def run_command_loop(self, shell_cmd, loop_times=10) :
        # tdLog.info(f"run command: {command}")
        shell_cmd = shell_cmd  + "  > /dev/null 2>&1" 
        for i in range(loop_times):
            os.system(shell_cmd )
            sleep(1)

    def user_privilege_check(self):
        host = socket.gethostname()
        comd1 = f"curl -u {self.user}:{self.passwd} -d \"create table db.jtable (ts timestamp, c1 VARCHAR(64))\"   {host}:6041/rest/sql"
        comd_select = f"curl -u {self.user}:{self.passwd}   -d \"select * from information_schema.ins_user_privileges;\"   {host}:6041/rest/sql"       
        self.run_command_failed(comd1)
        # t = threading.Thread(target=self.run_command_loop, args=(comd_select,10,))
        tdSql.execute(f'grant all on *.* to {self.user};')
        time.sleep(5)
        # t.start()
        tdSql.query("select * from information_schema.ins_user_privileges;")
        tdLog.debug(tdSql.queryResult)
        comd2 = f"curl -u {self.user}:{self.passwd}  -d \"create database db1\"   {host}:6041/rest/sql" 
        self.run_command_success(comd2)
        self.run_command_success(comd1)

        tdSql.execute(f"alter user root pass \"{self.passwd}\" ")
        time.sleep(3)
        comd3 = f"curl -u {self.user}:{self.passwd} -d \"create table db.t1 (ts timestamp, c1 VARCHAR(64))\"   {host}:6041/rest/sql" 
        self.run_command_success(comd3)
        # t.join()
    def run(self):
        self.create_user()
        self.prepare_data()
        self.user_privilege_check()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
