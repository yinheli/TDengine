# -*- coding: utf-8 -*-
import requests
import configparser
import os
import json


class RequestUtil(object):
    def __init__(self):
        self.user = None
        self.password = None
        self.database = None
        self.host = None
        self.rest_port = None
        self.port = None
        self.base_url = None

        # 读取配置文件
        confile = os.path.join(os.path.dirname(__file__), "conf", "config.ini")
        cf = configparser.ConfigParser()
        cf.read(confile, encoding='UTF-8')

        # 读取元数据库链接信息
        self.ldapServer = cf.get("metadateDB", "server")
        self.port = cf.get("metadateDB", "port")
        self.user = cf.get("metadateDB", "user")
        self.password = cf.get("metadateDB", "passwd")
        self.ldapServer = cf.get("metadateDB", "database")
        self.rest_port = cf.get("metadateDB", "rest_port")

        self.base_url = "{0}:{1}/rest/sql".format(self.ldapServer, self.rest_port)

    def executeQuery(self, sql: str):
        pass
        # data = json.dumps({'Authorization': sql})
        # r = requests.post(self.base_url, data)
        # print(r.json)

        # for row in rs:
        #     rowMap = {}
        #     for i in range(0, len(row)):
        #         rowMap[field_names[i]] = row[i]
        #     ret.append(rowMap)
        # return ret

    def execute(self, sql: str, data=None):
        cursor = self.dbHandler.cursor()
        if data is None:
            cursor.execute(sql)
        else:
            cursor.execute(sql, data)
        cursor.close()

    def executemany(self, sql: str, data):
        cursor = self.dbHandler.cursor()
        cursor.executemany(sql, data)
        cursor.close()

    def commit(self):
        self.dbHandler.commit()

    def rollback(self):
        self.dbHandler.rollback()

    def format_output_tab(self, rowset, startIter: int = 0):
        def wide_chars(s):
            # 判断字符串中包含的中文字符数量
            if isinstance(s, str):
                # W  宽字符
                # F  全角字符
                # H  半角字符
                # Na  窄字符
                # A   不明确的
                # N   正常字符
                return sum(unicodedata.east_asian_width(x) in ['W', 'F'] for x in s)
            else:
                return 0

        # 将屏幕输出按照表格进行输出
        # 记录每一列的最大显示长度
        m_ColumnLength = []

        if self:
            pass

        # 空结果集直接返回
        if rowset is None or len(rowset) == 0:
            return

        # 获取表头信息（字段名称）
        headers = list(rowset[0].keys())

        # 获取列的数据类型
        columnTypes = []
        for col in list(rowset[0].values()):
            if type(col) in [int, float]:
                columnTypes.append("NUMERIC")
            else:
                columnTypes.append("STRING")

        # 首先将表头的字段长度记录其中
        for m_Header in headers:
            m_ColumnLength.append(len(m_Header) + wide_chars(m_Header))

        # 查找列的最大字段长度
        for m_Row in rowset:
            m_Row = list(m_Row.values())
            for pos in range(0, len(m_Row)):
                if m_Row[pos] is None:
                    # 空值打印为<null>
                    if m_ColumnLength[pos] < len('<null>'):
                        m_ColumnLength[pos] = len('<null>')
                elif isinstance(m_Row[pos], str):
                    for m_iter in m_Row[pos].split('\n'):
                        if len(m_iter) + wide_chars(m_iter) > m_ColumnLength[pos]:
                            # 为了保持长度一致，长度计算的时候扣掉中文的显示长度
                            m_ColumnLength[pos] = len(m_iter) + wide_chars(m_iter)
                else:
                    if len(str(m_Row[pos])) + wide_chars(m_Row[pos]) > m_ColumnLength[pos]:
                        m_ColumnLength[pos] = len(str(m_Row[pos])) + wide_chars(m_Row[pos])

        # 打印表格上边框
        # 计算表格输出的长度, 开头有一个竖线，随后每个字段内容前有一个空格，后有一个空格加上竖线
        # 1 + [（字段长度+3） *]
        m_TableBoxLine = '+--------+'
        for m_Length in m_ColumnLength:
            m_TableBoxLine = m_TableBoxLine + (m_Length + 2) * '-' + '+'
        yield m_TableBoxLine

        # 打印表头以及表头下面的分割线
        m_TableContentLine = '|   ##   |'
        for pos in range(0, len(headers)):
            m_TableContentLine = \
                m_TableContentLine + \
                ' ' + str(headers[pos]).center(m_ColumnLength[pos] - wide_chars(headers[pos])) + ' |'
        yield m_TableContentLine
        yield m_TableBoxLine

        # 打印字段内容
        m_RowNo = startIter
        for m_Row in rowset:
            m_Row = list(m_Row.values())
            m_RowNo = m_RowNo + 1
            # 首先计算改行应该打印的高度（行中的内容可能右换行符号）
            m_RowHeight = 1
            for pos in range(0, len(m_Row)):
                if isinstance(m_Row[pos], str):
                    if len(m_Row[pos].split('\n')) > m_RowHeight:
                        m_RowHeight = len(m_Row[pos].split('\n'))
            # 首先构造一个空的结果集，行数为计划打印的行高
            m_output = []
            if m_RowHeight == 1:
                m_output.append(m_Row)
            else:
                for m_iter in range(0, m_RowHeight):
                    m_output.append(())
                # 依次填入数据
                for pos in range(0, len(m_Row)):
                    if isinstance(m_Row[pos], str):
                        m_SplitColumnValue = m_Row[pos].split('\n')
                    else:
                        m_SplitColumnValue = [m_Row[pos], ]
                    for m_iter in range(0, m_RowHeight):
                        if len(m_SplitColumnValue) > m_iter:
                            if str(m_SplitColumnValue[m_iter]).endswith('\r'):
                                m_SplitColumnValue[m_iter] = m_SplitColumnValue[m_iter][:-1]
                            m_output[m_iter] = m_output[m_iter] + (m_SplitColumnValue[m_iter],)
                        else:
                            m_output[m_iter] = m_output[m_iter] + ("",)
            m_RowNoPrinted = False
            for m_iter in m_output:
                m_TableContentLine = '|'
                if not m_RowNoPrinted:
                    m_TableContentLine = m_TableContentLine + str(m_RowNo).rjust(7) + ' |'
                    m_RowNoPrinted = True
                else:
                    m_TableContentLine = m_TableContentLine + '        |'
                for pos in range(0, len(m_iter)):
                    if m_iter[pos] is None:
                        m_PrintValue = '<null>'
                    else:
                        m_PrintValue = str(m_iter[pos])
                    if columnTypes is not None:
                        if columnTypes[pos] == "STRING":
                            # 字符串左对齐
                            m_TableContentLine = \
                                m_TableContentLine + ' ' + \
                                m_PrintValue.ljust(m_ColumnLength[pos] - wide_chars(m_PrintValue)) + ' |'
                        else:
                            # 数值类型右对齐, 不需要考虑wide_chars
                            m_TableContentLine = m_TableContentLine + ' ' + \
                                                 m_PrintValue.rjust(m_ColumnLength[pos]) + ' |'
                    else:
                        # 没有返回columntype, 按照字符串处理
                        m_TableContentLine = \
                            m_TableContentLine + ' ' + \
                            m_PrintValue.ljust(m_ColumnLength[pos] - wide_chars(m_PrintValue)) + ' |'
                yield m_TableContentLine

        # 打印表格下边框
        yield m_TableBoxLine

if __name__ == "__main__":
    headers = {"Authorization": "Basic cm9vdDp0YW9zZGF0YQ=="}
    # r = requests.post('http://192.168.1.204:6041/rest/sql')
    data = 'show databases'
    r = requests.post('http://192.168.1.204:6041/rest/sql', headers=headers, data=data)
    print(r.status_code)
    print(r.headers)
    print(r.text)