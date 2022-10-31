from util.log import *
from util.cases import *
from util.sql import *
import numpy as np
import random ,os ,sys
import platform
import math

class TDTestCase:
    updatecfgDict = {"maxTablesPerVnode":2 ,"minTablesPerVnode":2,"tableIncStepPerVnode":2 }

    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.vnode_disbutes = None
        self.ts = 1537146000000


    def check_stddev_functions(self, tbname , col_name):

        stddev_sql = f"select stddev({col_name}) from {tbname};"

        same_sql = f"select {col_name} from {tbname} where {col_name} is not null "

        tdSql.query(same_sql)
        pre_data = np.array(tdSql.queryResult)[np.array(tdSql.queryResult) != None]
        if (platform.system().lower() == 'windows' and pre_data.dtype == 'int32'):
            pre_data = np.array(pre_data, dtype = 'int64')
        pre_avg = np.sum(pre_data)/len(pre_data)

        # Calculate variance
        stddev_result = 0
        for num in tdSql.queryResult:
            stddev_result += (num-pre_avg)*(num-pre_avg)/len(tdSql.queryResult)

        stddev_result = math.sqrt(stddev_result)

        tdSql.query(stddev_sql)

        if -0.0001 < tdSql.queryResult[0][0]-stddev_result < 0.0001:
            tdLog.info(" sql:%s; row:0 col:0 data:%d , expect:%d"%(stddev_sql,tdSql.queryResult[0][0],stddev_result))
        else:
            tdLog.exit(" sql:%s; row:0 col:0 data:%d , expect:%d"%(stddev_sql,tdSql.queryResult[0][0],stddev_result))

    def prepare_datas_of_distribute(self, dbname="testdb"):

        # prepate datas for  20 tables distributed at different vgroups
        tdSql.execute(f"create database if not exists {dbname} keep 3650 duration 1000 vgroups 5")
        tdSql.execute(f" use {dbname}")
        tdSql.execute(
            f'''create table {dbname}.stb1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            tags (t0 timestamp, t1 int, t2 bigint, t3 smallint, t4 tinyint, t5 float, t6 double, t7 bool, t8 binary(16),t9 nchar(32))
            '''
        )

        tdSql.execute(
            f'''
            create table {dbname}.t1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp)
            '''
        )
        for i in range(20):
            tdSql.execute(f'create table {dbname}.ct{i+1} using {dbname}.stb1 tags ( now(), {1*i}, {11111*i}, {111*i}, {1*i}, {1.11*i}, {11.11*i}, {i%2}, "binary{i}", "nchar{i}" )')

        for i in range(9):
            tdSql.execute(
                f"insert into {dbname}.ct1 values ( now()-{i*10}s, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )
            tdSql.execute(
                f"insert into {dbname}.ct4 values ( now()-{i*90}d, {1*i}, {11111*i}, {111*i}, {11*i}, {1.11*i}, {11.11*i}, {i%2}, 'binary{i}', 'nchar{i}', now()+{1*i}a )"
            )

        for i in range(1,21):
            if i ==1 or i == 4:
                continue
            else:
                tbname = f"ct{i}"
                for j in range(9):
                    tdSql.execute(
                f"insert into {dbname}.{tbname} values ( now()-{(i+j)*10}s, {1*(j+i)}, {11111*(j+i)}, {111*(j+i)}, {11*(j)}, {1.11*(j+i)}, {11.11*(j+i)}, {(j+i)%2}, 'binary{j}', 'nchar{j}', now()+{1*j}a )"
            )

        tdSql.execute(f"insert into {dbname}.ct1 values (now()-45s, 0, 0, 0, 0, 0, 0, 0, 'binary0', 'nchar0', now()+8a )")
        tdSql.execute(f"insert into {dbname}.ct1 values (now()+10s, 9, -99999, -999, -99, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )")
        tdSql.execute(f"insert into {dbname}.ct1 values (now()+15s, 9, -99999, -999, -99, -9.99, NULL, 1, 'binary9', 'nchar9', now()+9a )")
        tdSql.execute(f"insert into {dbname}.ct1 values (now()+20s, 9, -99999, -999, NULL, -9.99, -99.99, 1, 'binary9', 'nchar9', now()+9a )")

        tdSql.execute(f"insert into {dbname}.ct4 values (now()-810d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(f"insert into {dbname}.ct4 values (now()-400d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(f"insert into {dbname}.ct4 values (now()+90d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL  ) ")

        tdSql.execute(
            f'''insert into {dbname}.t1 values
            ( '2020-04-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( '2020-10-21 01:01:01.000', 1, 11111, 111, 11, 1.11, 11.11, 1, "binary1", "nchar1", now()+1a )
            ( '2020-12-31 01:01:01.000', 2, 22222, 222, 22, 2.22, 22.22, 0, "binary2", "nchar2", now()+2a )
            ( '2021-01-01 01:01:06.000', 3, 33333, 333, 33, 3.33, 33.33, 0, "binary3", "nchar3", now()+3a )
            ( '2021-05-07 01:01:10.000', 4, 44444, 444, 44, 4.44, 44.44, 1, "binary4", "nchar4", now()+4a )
            ( '2021-07-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            ( '2021-09-30 01:01:16.000', 5, 55555, 555, 55, 5.55, 55.55, 0, "binary5", "nchar5", now()+5a )
            ( '2022-02-01 01:01:20.000', 6, 66666, 666, 66, 6.66, 66.66, 1, "binary6", "nchar6", now()+6a )
            ( '2022-10-28 01:01:26.000', 7, 00000, 000, 00, 0.00, 00.00, 1, "binary7", "nchar7", "1970-01-01 08:00:00.000" )
            ( '2022-12-01 01:01:30.000', 8, -88888, -888, -88, -8.88, -88.88, 0, "binary8", "nchar8", "1969-01-01 01:00:00.000" )
            ( '2022-12-31 01:01:36.000', 9, -99999999999999999, -999, -99, -9.99, -999999999999999999999.99, 1, "binary9", "nchar9", "1900-01-01 00:00:00.000" )
            ( '2023-02-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )
            '''
        )

        tdLog.info(f" prepare data for distributed_aggregate done! ")

    def check_distribute_datas(self, dbname="testdb"):
        # get vgroup_ids of all
        tdSql.query(f"show {dbname}.vgroups ")
        vgroups = tdSql.queryResult

        vnode_tables={}

        for vgroup_id in vgroups:
            vnode_tables[vgroup_id[0]]=[]

        # check sub_table of per vnode ,make sure sub_table has been distributed
        tdSql.query(f"select * from information_schema.ins_tables where db_name = '{dbname}' and table_name like 'ct%'")
        table_names = tdSql.queryResult
        tablenames = []
        for table_name in table_names:
            vnode_tables[table_name[6]].append(table_name[0])
        self.vnode_disbutes = vnode_tables

        count = 0
        for k ,v in vnode_tables.items():
            if len(v)>=2:
                count+=1
        if count < 2:
            tdLog.exit(f" the datas of all not satisfy sub_table has been distributed ")

    def check_stddev_distribute_diff_vnode(self,col_name, dbname="testdb"):

        vgroup_ids = []
        for k ,v in self.vnode_disbutes.items():
            if len(v)>=2:
                vgroup_ids.append(k)

        distribute_tbnames = []

        for vgroup_id in vgroup_ids:
            vnode_tables = self.vnode_disbutes[vgroup_id]
            distribute_tbnames.append(random.sample(vnode_tables,1)[0])
        tbname_ins = ""
        for tbname in distribute_tbnames:
            tbname_ins += "'%s' ,"%tbname

        tbname_filters = tbname_ins[:-1]

        stddev_sql = f"select stddev({col_name}) from {dbname}.stb1 where tbname in ({tbname_filters});"

        same_sql = f"select {col_name}  from {dbname}.stb1 where tbname in ({tbname_filters}) and {col_name} is not null "

        tdSql.query(same_sql)
        pre_data = np.array(tdSql.queryResult)[np.array(tdSql.queryResult) != None]
        if (platform.system().lower() == 'windows' and pre_data.dtype == 'int32'):
            pre_data = np.array(pre_data, dtype = 'int64')
        pre_avg = np.sum(pre_data)/len(pre_data)

        # Calculate variance
        stddev_result = 0
        for num in tdSql.queryResult:
            stddev_result += (num-pre_avg)*(num-pre_avg)/len(tdSql.queryResult)

        stddev_result = math.sqrt(stddev_result)

        tdSql.query(stddev_sql)
        tdSql.checkData(0,0,stddev_result)

    def check_stddev_status(self, dbname="testdb"):
        # check max function work status

        tdSql.query(f"show {dbname}.tables like 'ct%'")
        table_names = tdSql.queryResult
        tablenames = []
        for table_name in table_names:
            tablenames.append(f"{dbname}.{table_name[0]}")

        tdSql.query(f"desc {dbname}.stb1")
        col_names = tdSql.queryResult

        colnames = []
        for col_name in col_names:
            if col_name[1] in ["INT" ,"BIGINT" ,"SMALLINT" ,"TINYINT" , "FLOAT" ,"DOUBLE"]:
                colnames.append(col_name[0])

        for tablename in tablenames:
            for colname in colnames:
                if colname.startswith("c"):
                    self.check_stddev_functions(tablename,colname)

        # check max function for different vnode

        for colname in colnames:
            if colname.startswith("c"):
                self.check_stddev_distribute_diff_vnode(colname)

    def distribute_agg_query(self, dbname="testdb"):
        # basic filter
        tdSql.query(f"select stddev(c1) from {dbname}.stb1 ")
        tdSql.checkData(0,0,6.694663959)

        tdSql.query(f"select stddev(a) from (select stddev(c1) a  from {dbname}.stb1 partition by tbname) ")
        tdSql.checkData(0,0,0.156797505)

        tdSql.query(f"select stddev(c1) from {dbname}.stb1 where t1=1")
        tdSql.checkData(0,0,2.581988897)

        tdSql.query(f"select stddev(c1+c2) from {dbname}.stb1 where c1 =1 ")
        tdSql.checkData(0,0,0.000000000)

        tdSql.query(f"select stddev(c1) from {dbname}.stb1 where tbname=\"ct2\"")
        tdSql.checkData(0,0,2.581988897)

        tdSql.query(f"select stddev(c1) from {dbname}.stb1 partition by tbname")
        tdSql.checkRows(20)

        tdSql.query(f"select stddev(c1) from {dbname}.stb1 where t1> 4  partition by tbname")
        tdSql.checkRows(15)

        # union all
        tdSql.query(f"select stddev(c1) from {dbname}.stb1 union all select stddev(c1) from {dbname}.stb1 ")
        tdSql.checkRows(2)
        tdSql.checkData(0,0,6.694663959)

        tdSql.query(f"select stddev(a) from (select stddev(c1) a from {dbname}.stb1 union all select stddev(c1) a  from {dbname}.stb1)")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,0.000000000)

        # join

        tdSql.execute(" create database if not exists db ")
        tdSql.execute(" use db ")
        tdSql.execute(" create stable db.st (ts timestamp , c1 int ,c2 float) tags(t1 int) ")
        tdSql.execute(" create table db.tb1 using db.st tags(1) ")
        tdSql.execute(" create table db.tb2 using db.st tags(2) ")


        for i in range(10):
            ts = i*10 + self.ts
            tdSql.execute(f" insert into db.tb1 values({ts},{i},{i}.0)")
            tdSql.execute(f" insert into db.tb2 values({ts},{i},{i}.0)")

        tdSql.query("select stddev(tb1.c1), stddev(tb2.c2) from db.tb1 tb1, db.tb2 tb2 where tb1.ts=tb2.ts")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,2.872281323)
        tdSql.checkData(0,1,2.872281323)

        # group by
        tdSql.execute(f" use {dbname} ")

        # partition by tbname or partition by tag
        tdSql.query(f"select stddev(c1) from {dbname}.stb1 partition by tbname")
        tdSql.checkRows(20)

        # nest query for support max
        tdSql.query(f"select stddev(c2+2)+1 from (select stddev(c1) c2  from {dbname}.stb1)")
        tdSql.checkData(0,0,1.000000000)
        tdSql.query(f"select stddev(c1+2)  as c2 from (select ts ,c1 ,c2  from {dbname}.stb1)")
        tdSql.checkData(0,0,6.694663959)
        tdSql.query(f"select stddev(a+2)  as c2 from (select ts ,abs(c1) a ,c2  from {dbname}.stb1)")
        tdSql.checkData(0,0,6.694663959)

        # mixup with other functions
        tdSql.query(f"select max(c1),count(c1),last(c2,c3),sum(c1+c2),avg(c1),stddev(c1) from {dbname}.stb1")
        tdSql.checkData(0,0,28)
        tdSql.checkData(0,1,184)
        tdSql.checkData(0,2,-99999)
        tdSql.checkData(0,3,-999)
        tdSql.checkData(0,4,28202310.000000000)
        tdSql.checkData(0,5,14.086956522)
        tdSql.checkData(0,6,6.694663959)

    def run(self):

        self.prepare_datas_of_distribute()
        self.check_distribute_datas()
        self.check_stddev_status()
        self.distribute_agg_query()


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
