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
        autoGen.create_stable(self.stb, 14, 14, 8, 8)
        autoGen.create_child(self.stb, "dt", self.childtable_count)
        autoGen.insert_data(10)

        autoGen = AutoGen()
        autoGen.create_stable('astb', 14, 14, 8, 8)
        autoGen.insert_data(10, self.childtable_count, "adt")

        sql1 = f"select * from {self.stb}"
        sql2 = f"select * from a{self.stb}"
        self.checkSameResult(sql1, sql2)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
