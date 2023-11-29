# -*- coding: utf-8 -*-
import unicodedata


class PrintUtil(object):

    def __init__(self):
        pass

    def format_output_csv(self, headers, columnTypes, cur):
        # 将屏幕输出按照CSV格式进行输出
        m_csv_delimiter = self.testOptions.get("OUTPUT_CSV_DELIMITER")
        m_csv_quotechar = self.testOptions.get("OUTPUT_CSV_QUOTECHAR")
        if m_csv_delimiter.find("\\t") != -1:
            m_csv_delimiter = m_csv_delimiter.replace("\\t", '\t')
        if m_csv_delimiter.find("\\s") != -1:
            m_csv_delimiter = m_csv_delimiter.replace("\\s", ' ')

        # 打印字段名称
        if self.testOptions.get("OUTPUT_CSV_HEADER") == "ON":
            m_row = ""
            for pos in range(0, len(headers)):
                m_row = m_row + str(headers[pos])
                if pos != len(headers) - 1:
                    m_row = m_row + m_csv_delimiter
            yield str(m_row)

        # 打印字段内容
        for row in cur:
            m_row = ""
            for pos in range(0, len(row)):
                if row[pos] is None:
                    if columnTypes is not None:
                        if columnTypes[pos] in ("VARCHAR", "LONGVARCHAR", "CHAR", "CLOB", "NCLOB"):
                            m_row = m_row + m_csv_quotechar + m_csv_quotechar
                else:
                    if columnTypes is None:
                        m_row = m_row + str(row[pos])
                    else:
                        if columnTypes[pos] in ("VARCHAR", "LONGVARCHAR", "CHAR", "CLOB", "NCLOB"):
                            m_row = m_row + m_csv_quotechar + str(row[pos]) + m_csv_quotechar
                        else:
                            m_row = m_row + str(row[pos])
                if pos != len(row) - 1:
                    m_row = m_row + m_csv_delimiter
            yield str(m_row)

    def format_output_tab(self, headers, columnTypes, cur, startIter: int = 0):
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

        if self:
            pass
        # 将屏幕输出按照表格进行输出
        # 记录每一列的最大显示长度
        m_ColumnLength = []
        # 首先将表头的字段长度记录其中
        for m_Header in headers:
            m_ColumnLength.append(len(m_Header) + wide_chars(m_Header))
        # 查找列的最大字段长度
        for m_Row in cur:
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
        for m_Row in cur:
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
                        if columnTypes[pos] in \
                                ("NVARCHAR", "NCHAR", "VARCHAR", "LONGVARCHAR", "CHAR",
                                 "CLOB", "NCLOB", "STRUCT", "ARRAY", "DATE"):
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
    a = PrintUtil()
    aa = a.format_output_tab(headers=['header1','header2'], columnTypes=['varchar','int'], cur=[('aaaaaaaaaaaaaa','123')], startIter=0)
    for bb in aa:

        print(bb)