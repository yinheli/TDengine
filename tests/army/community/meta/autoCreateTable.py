import taos
import sys
import os
import subprocess
import glob
import shutil
import time

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.srvCtl import *
from frame.caseBase import *
from frame import *
from frame.autogen import *


class TDTestCase(TBase):

    def init(self, conn, logSql, replicaVar=3):
        super(TDTestCase, self).init(conn, logSql)
        self.valgrind = 0
        self.childtable_count = 1
        tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def run(self):
        tdSql.prepare()

        tdSql.execute(f"use {self.db}")
        autoGen = AutoGen()
        autoGen.create_stable(self.stb, 15, 15, 8, 8)
        autoGen.create_child(self.stb, "dt", self.childtable_count, '001', True)
        autoGen.insert_data(10, False, '001')

        autoGen.mtags
        autoGen = AutoGen()
        autoGen.create_stable('astb', 15, 15, 8, 8)
        autoGen.insert_data_auto_create(10, self.childtable_count, "adt", False, '001', '001', True)
        bCheck = True
        for i in range(1, 15):
            # sql1 = f"select t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13 from {self.stb} where t{i}=001"
            # sql2 = f"select t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13 from a{self.stb} where t{i}=001"
            sql1 = f"select t{i} from {self.stb} where t{i}=001"
            sql2 = f"select t{i} from a{self.stb} where t{i}=001"
            result = self.checkSameResult(sql1, sql2, False)
            if bCheck and not result:
                bCheck = False

            sql1 = f"select t{i} from {self.stb} where t{i}='001'"
            sql2 = f"select t{i} from a{self.stb} where t{i}='001'"
            result = self.checkSameResult(sql1, sql2, False)
            if bCheck and not result:
                bCheck = False

        if not bCheck:
            tdLog.exit("checkSameResult column value different")

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
