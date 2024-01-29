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

import sys
import time
import random

import taos
import frame
import frame.etool
import json
import threading

from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
from frame.autogen import *
from frame.srvCtl import *
from taos.tmq import Consumer


class TDTestCase(TBase):
    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug(f"start to init {__file__}")
        super(TDTestCase, self).init(conn, logSql, replicaVar)

        self.configJsonFile('changeMaster.json')
        tdSql.execute('drop database if exists db')
        tdSql.execute("drop stream if exists ma")
        autogen = AutoGen()
        autogen.create_db('db', 1, 3)
        tdSql.execute(f"use {self.db}")
        self.modifyConfigJsonFile('changeMaster.json', insert_rows=0)
        self.dbInsertThread('changeMaster.json', None)
        self.insert_rows = 100000
        self.modifyConfigJsonFile('changeMaster.json', insert_rows=self.insert_rows, child_table_exists='yes')
        self.createStream()

    def createStream(self):
        tdSql.execute(
            "create stream if not exists ma trigger WINDOW_CLOSE fill_history 1 IGNORE EXPIRED 0 IGNORE UPDATE 0  into sta as select _wstart as wstart, _wend, count(*) as item_count, sum(ic) as item_sum, avg(ic) as item_avg from db.stb partition by tbname interval(5s);")
        tdLog.debug("tmqStreamThread create stream finish!")
        time.sleep(2)

    def modifyConfigJsonFile(self, fileName, dbName='db', vgroups=1, replica=3, insert_rows=10000000,
                             timestamp_step=1000, child_table_exists='no'):
        tdLog.debug(f"modifyConfigJsonFile {fileName}")
        filePath = etool.curFile(__file__, fileName)
        with open(filePath, 'r') as f:
            data = json.load(f)
        data['databases'][0]['dbinfo']['name'] = dbName
        data['databases'][0]['dbinfo']['vgroups'] = vgroups
        data['databases'][0]['dbinfo']['replica'] = replica
        data['databases'][0]['super_tables'][0]['insert_rows'] = insert_rows
        data['databases'][0]['super_tables'][0]['timestamp_step'] = timestamp_step
        data['databases'][0]['super_tables'][0]['child_table_exists'] = child_table_exists
        json_data = json.dumps(data)
        with open(filePath, "w") as file:
            file.write(json_data)

        tdLog.debug(f"modifyConfigJsonFile {json_data}")

    def configJsonFile(self, fileName):
        tdLog.debug(f"configJsonFile {fileName}")
        filePath = etool.curFile(__file__, fileName)
        with open(filePath, 'r') as f:
            data = json.load(f)

        self.insert_rows = data['databases'][0]['super_tables'][0]['insert_rows']
        self.childtable_count = data['databases'][0]['super_tables'][0]['childtable_count']
        self.timestamp_step = data['databases'][0]['super_tables'][0]['timestamp_step']

    # def changeMasterThread(self, configFile, event):
    #     while not event.isSet():
    #         self.dNodeStop()
    #         time.sleep(5)

    def dNodeStop(self):
        queryRows = tdSql.query('show vnodes')
        dnodeId = 1
        if queryRows > 0:
            for i in range(queryRows):
                status = tdSql.getData(i, 3)
                if str(status) == 'leader':
                    dnodeId = tdSql.getData(i, 0)
                    tdLog.debug(dnodeId)
                    #   dnodeId = self.getDnodeInfo("", dnodeId)
                    sc.dnodeStop(dnodeId)
                    time.sleep(10)
                    sc.dnodeStart(dnodeId)
                    break

        time.sleep(5)
        queryRows = tdSql.query('show mnodes')
        if queryRows > 0:
            for i in range(queryRows):
              status = tdSql.getData(i, 2)
              if str(status) == 'leader':
                  endpoint = tdSql.getData(i, 1)
                  tdLog.debug(endpoint)
                #   dnodeId = self.getDnodeInfo(endpoint, -1)
                  sc.dnodeStop(dnodeId)
                  time.sleep(5)
                  sc.dnodeStart(dnodeId)
                  break
        if dnodeId is None:
            tdLog.exit(f"no find master dnode!")

    def getDnodeInfo(self, endpoint, id):
        queryRows = 0
        dnodeId = None
        if len(endpoint) > 0:
            queryRows = tdSql.query(
                f"select * from information_schema.ins_dnodes where endpoint = '{endpoint}' and status='ready'")
        elif id >= 0:
            queryRows = tdSql.query(f"select * from information_schema.ins_dnodes where id = {id} and status='ready'")

        if queryRows > 0:
            dnodeId = tdSql.getData(0, 0)

        if dnodeId is None:
            tdLog.exit(f"get master dnode info fail! id:{id}, endpoint:{endpoint}")
        return dnodeId

    def dbQueryThread(self, configFile, event):
        streamSum = 0
        while not event.isSet():
            queryRows = tdSql.query("select count(*), sum(ic), avg(ic) from db.stb partition by ic")
            if queryRows > 0:
                count = tdSql.getData(0, 0)
                sum = tdSql.getData(0, 1)
                avg = tdSql.getData(0, 2)
                tdLog.debug(f"dbQueryThread count:{count}, sum:{sum}, avg:{avg}")

            streamSum = self.tmqStreamQuery()
            self.dNodeStop()
            time.sleep(5)

        streamSum = self.tmqStreamQuery()
        if streamSum != self.childtable_count * self.insert_rows:
            tdLog.exit(
                f"tmqStreamThread count != insert_rows, sum:{streamSum}, insert_rows:{self.childtable_count * self.insert_rows}")

        self.checkInsertCorrect()

    def dbInsertThread(self, configFile, event):
        tdLog.debug(f"dbInsertThread start {configFile}")
        jfile = etool.curFile(__file__, configFile)
        etool.benchMark(json=jfile)
        if not event is None:
            time.sleep(5)
            event.set()

    def tmqCreateConsumer(self):
        consumer_dict = {
            "group.id": "1",
            "client.id": "client",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.commit.interval.ms": "1000",
            "enable.auto.commit": "true",
            "auto.offset.reset": "earliest",
            "experimental.snapshot.enable": "false",
            "msg.with.table.name": "false"
        }

        consumer = Consumer(consumer_dict)
        try:
            consumer.subscribe(["db_change_master_topic"])
        except Exception as e:
            tdLog.info("%s" % (e))
            tdLog.exit("consumer.subscribe() fail ")

        tdLog.info("create consumer success!")
        return consumer

    def tmqSubscribeThread(self, configFile, event):
        sum = 0
        tdSql.execute("create topic db_change_master_topic as select * from db.stb;")
        consumer = self.tmqCreateConsumer()
        try:
            for i in range(10):
                while True:
                    res = consumer.poll(1)
                    if not res:
                        break
                    err = res.error()
                    if err is not None:
                        raise err
                    val = res.value()

                    for block in val:
                        sum += len(block.fetchall())
                time.sleep(5)
                tdLog.info(f"tmqSubscribeThread Subscribe {i} sum:{sum}")
        finally:
            consumer.unsubscribe()
            consumer.close()

        tdLog.info(f"tmqSubscribeThread Subscribe sum:{sum}")
        if sum != self.childtable_count * self.insert_rows:
            tdLog.exit(
                f"tmqSubscribeThread sum != insert_rows, sum:{sum}, insert_rows:{self.childtable_count * self.insert_rows}")

    def tmqStreamQuery(self):
        count = 0
        queryRows = tdSql.query("select sum(item_count) from db.sta")
        if queryRows > 0:
            count = tdSql.getData(0, 0)
            tdLog.debug(f"tmqStreamThread sum:{count}")
        return count

        # run

    def run(self):
        tdLog.debug(f"start to excute {__file__}")
        event = threading.Event()
        t1 = threading.Thread(target=self.dbInsertThread, args=('changeMaster.json', event))
        t2 = threading.Thread(target=self.dbQueryThread, args=('changeMaster.json', event))
        t3 = threading.Thread(target=self.tmqSubscribeThread, args=('changeMaster.json', event))
        t1.start()
        time.sleep(2)
        t2.start()
        t3.start()
        tdLog.debug("threading started!!!!!")
        t1.join()
        t2.join()
        t3.join()
        tdLog.success(f"{__file__} successfully executed")

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())