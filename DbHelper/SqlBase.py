from buildings.comm_across import *
from sqlalchemy import create_engine
import pymysql
import pandas as pd
import json
import pymssql

from pyhive import hive


class MysqlPyc:

    def __init__(self, db_dic, with_conn=True):
        self.db_dic = db_dic
        self.default_dic = {'port': 3306, 'charset': 'utf8', 'database': 'mysql'}
        self.conn, self.cur, self.engine = None, None, None
        self.engine_state = "mysql+pymysql://%(user)s:%(passwd)s@%(host)s:%(port)s/%(database)s?charset=%(charset)s"
        self.wait = 4  # 为了闪断自动重连
        self.module = pymysql
        self.module_cursor = self.module.cursors.DictCursor
        self.table = ''
        if with_conn:
            self.c_conn()

    def wait_for_(self, err_flag, err):
        boob = self.wait < 19
        if boob:
            # 1507 - Error in list of partitions to DROP
            if type(err.args[0]) is int and err.args[0] in [1062, 1507]:
                print(err.args[0], 'err_num pass')
                return False
            for x in ['already exists', "doesn't exist", 'Duplicate ', 'error in your SQL', 'partitions to DROP']:
                if type(err.args[0]) is str and re.findall(x, err.args[0]):  # ex_exceptions 不再继续等待重做的例外
                    print(x, err.args[0], 'pass')
                    return False

            print(day_forpast(0, ss='%Y-%m-%d %H:%M:%S'), 'Exception:flag(%s):' % err_flag)
            print('   ', err, 'waiting for %s .... ' % self.wait, )
            time.sleep(self.wait)
            self.wait += 1
        else:
            self.quit('重连了太多次,aborted！')
        return boob

    def c_conn(self):  # change or create conn
        try:
            for k, v in self.default_dic.items():
                if k not in self.db_dic:
                    self.db_dic[k] = v
            self.conn = self.module.connect(**self.db_dic)
            self.cur = self.conn.cursor(self.module_cursor)
            self.engine = self.engine_state % self.db_dic
            return self

        except Exception as err:
            if self.wait_for_('c_conn', err):
                return self.c_conn()

    def get_a_table(self, sql):
        if self.table == '':
            res_ss = re.findall('from\s+(\S+?)[\s;]', sql)
            if res_ss:
                self.table = res_ss[0]
        return self.table

    def do_single_sql(self, sql):
        self.cur.execute(sql)
        self.conn.commit()
        rows = self.cur.fetchall()
        return rows

    def _pre_do_sqls(self, sqls):
        # 长sql预处理,输入是长字符的sql,多段sql之间可以是;分割,处理前面的并返回最后一段sql
        sqls_li = sqls if type(sqls) is list else sqls.split(';')
        # 去掉结尾长度很小的无效sql
        while len(sqls_li) and len(re.sub('\s', '', sqls_li[-1])) < 6:
            sqls_li.pop(-1)
        for sql in sqls_li[:-1]:  #
            if len(re.sub('\s', '', sql)) > 6:
                _ = self.do_single_sql(sql)
        return sqls_li[-1] if len(sqls_li) else None

    def get_data(self, sqls):
        try:
            last_sql = self._pre_do_sqls(sqls)
            self.get_a_table(last_sql)
            return self.do_single_sql(last_sql)
        except Exception as err:
            if self.wait_for_('get_data', err):
                return self.c_conn().get_data(sqls)
            else:
                return None

    def to_dataframe(self, sqls):
        #  sqls   允许用;间隔多个sql,并可返回最后一个sql的返回结果去转换成dataframe
        try:
            last_sql = self._pre_do_sqls(sqls)
            self.get_a_table(last_sql)
            df = pd.read_sql(last_sql, self.conn)
            self.conn.commit()
            return df
        except Exception as err:
            if self.wait_for_('to_dataframe', err):
                return self.c_conn().to_dataframe(sqls)
            else:
                return pd.DataFrame([])

    def df_tosql(self, df, db_table=''):
        if db_table == '':
            db_table = self.table
            if db_table == '':
                return 'No table specified ~'
        connect = self.sql_engine()
        database, table_name = db_table.split('.')
        pd.io.sql.to_sql(df, table_name, connect, database, if_exists='append', index=False, chunksize=10000)

    def df_upd_tosql(self, df, batch=666, table=''):
        if table == '':
            table = self.table
            if table == '':
                return 'No table specified ~'
        df = df.fillna('Null')  # df的空值插入值 用inplace=True会报异常(解释待定)
        df_col_li = df.columns.get_level_values(0)
        cols = ','.join(df_col_li)
        valst = ','.join(['%s=VALUES(%s)' % (x, x) for x in df_col_li])
        res_li = []
        while len(df):
            ind = df.index[:batch]
            values = ','.join(['(%s)' % ','.join([str(df.loc[i, c]) for c in df_col_li]) for i in ind])
            sqls = 'INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s' % (table, cols, values, valst)
            # print(sqls)
            res_li += [self.sql_engine(sqls)]
            df.drop(ind, inplace=True)
        return res_li

    def df_replace_tosql(self, df, table='', batch=1000):
        if table == '':
            table = self.table
            if table == '':
                return 'No table specified ~'
        df_col_li = df.columns.get_level_values(0)
        cols = ','.join(df_col_li)
        res_li = []
        while len(df):
            ind = df.index[:batch]
            values = ','.join(['(%s)' % ','.join([str(df.loc[i, c]) for c in df_col_li]) for i in ind])
            sqls = 'replace into %s (%s) VALUES %s ' % (table, cols, values)
            res_li += [self.sql_engine(sqls)]
            df.drop(ind, inplace=True)
        return res_li

    def di_upd_tosql(self, di, table=''):
        if table == '':
            table = self.table
            if table == '':
                return 'No table specified ~'
        col_li, val_li = [], []
        for k, v in di.items():
            col_li += [k]
            val_li += [str(v)]
        cols = ','.join(col_li)
        vals = ','.join(val_li)
        valst = ','.join(['%s=VALUES(%s)' % (x, x) for x in col_li])
        sqls = 'INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s' % (table, cols, vals, valst)
        return self.sql_engine(sqls)

    def dicts_upd_tosql(self, dicts, table='', batch=1000):
        if table == '':
            table = self.table
            if table == '':
                return 'No table specified ~'
        if len(dicts):
            col_li = list(dicts[0].keys())
            cols = ','.join(col_li)
            valst = ','.join(['%s=VALUES(%s)' % (x, x) for x in col_li])
            res_li = []
            while len(dicts):
                do_dicts = dicts[:batch]
                values = ','.join(
                    ['(%s)' % ','.join([str(do_dicts[i][x]) for x in col_li]) for i in range(len(do_dicts))])
                sqls = 'INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s' % (table, cols, values, valst)
                res_li += [self.sql_engine(sqls)]
                dicts = dicts[batch:]
            return res_li

    def sql_engine(self, sqls=None, echo=False):
        try:
            # eng = create_engine(self.engine, echo=echo, encoding=self.db_dic['charset'])
            eng = create_engine(self.engine, echo=echo, encoding='utf8')
            # eng.execute('SET NAMES utf8')
            # eng.execute('SET CHARACTER SET utf8')
            # eng.execute('SET character_set_connection=%s;' %self.db_dic['charset'])
            # eng.execute("SHOW VARIABLES LIKE '%character%';")
            if sqls:
                sqls = self._pre_do_sqls(sqls)
                return eng.execute(sqls).rowcount if sqls else eng
            else:
                return eng
        except Exception as err:
            if self.wait_for_('sql_engine', err):
                return self.c_conn().sql_engine(sqls)

    def add_partition_gen(self, pname, pvalue, table='', tp=1):
        if table == '':
            table = self.table
            if table == '':
                return 'No table specified ~'
        if tp == 1:
            add_partition_gen1 = "ALTER TABLE %s ADD PARTITION (PARTITION p%s VALUES LESS THAN (%s));"
            return self.sql_engine(add_partition_gen1 % (table, pname, pvalue))

        if tp == 'list':
            add_partition_gen1 = "ALTER TABLE %s ADD PARTITION (PARTITION p%s VALUES IN (%s));"
            return self.sql_engine(add_partition_gen1 % (table, pname, pvalue))

    def drop_partition(self, pname, table=''):
        if table == '':
            table = self.table
            if table == '':
                return 'No table specified ~'
        return self.sql_engine('alter table %s drop PARTITION %s' % (table, pname))

    def get_partition(self, db_table):  # db_table 需要是库.表名格式传入
        database, table_name = db_table.split('.')
        sqls = """
        SELECT TABLE_SCHEMA,TABLE_NAME,partition_name,partition_expression,partition_description,table_rows  
        FROM INFORMATION_SCHEMA.partitions 
        WHERE TABLE_SCHEMA = '%s' and TABLE_NAME='%s'""" % (database, table_name)
        return self.to_dataframe(sqls)

    def init_bigtable_bypart(self, sqls):
        self.get_a_table(sqls)
        df_part = self.get_partiotion(self.table)
        df_list = []
        print('总分区数：', df_part.partition_name.__len__())
        for part in df_part.partition_name:
            print('%s partition (%s)' % (self.table, part))
            part_sqls = sqls.replace(self.table, '%s partition (%s)' % (self.table, part))
            df_list += self.to_dataframe(part_sqls)
        return df_list

    def quit(self, message=''):
        try:
            if message:
                print(message)
            # self.cur.close()
            self.conn.close()
        except Exception as err:
            print(day_forpast(0, ss='%Y-%m-%d %H:%M:%S'), 'Exception:flag(quit):', err.args[0])

    # def __del__(self):
    #     self.quit()


class MssqlPyc(MysqlPyc):

    def __init__(self, db_dic):
        super(MssqlPyc, self).__init__(db_dic, False)
        # ----- SqlServer 需要不同的地方 ------------
        self.default_dic = {'port': 1433, 'charset': 'UTF-8'}
        self.engine_state = "mssql+pymssql://%(user)s:%(password)s@%(host)s:%(port)s/%(database)s?charset=%(charset)s"
        self.module = pymssql
        self.module_cursor = None
        # ----- SqlServer 需要不同的地方 ------------
        self.c_conn()

    def get_data_(self, sqls):
        if type(sqls) is list:
            for sql in sqls:
                self.cur.execute(sql)
        else:
            self.cur.execute(sqls)
            self.get_a_table(sqls)
        rows = self.cur.fetchall()
        self.conn.commit()
        return rows

    def get_data(self, sql):
        df = self.to_dataframe(sql)
        return json.loads(df.to_json(orient='records'))


class HivePyc:

    def __init__(self, db_dic, with_conn=True):
        self.db_dic = db_dic
        self.default_dic = {'port': 10000}
        self.conn, self.cur, self.engine = None, None, None
        self.module = hive
        if 'password' in self.db_dic:
            self.engine_state = "hive://%(username)s:%(password)s@%(host)s:%(port)s/%(database)s"
        else:
            self.engine_state = "hive://%(username)s@%(host)s:%(port)s/%(database)s"
        self.wait = 5
        self.table = ''
        if with_conn:
            self.c_conn()

    def wait_for_(self, err_flag, err):
        boob = self.wait < 9
        if boob:
            for x in ['already exists', "doesn't exist", 'Duplicate ', 'error in your SQL', 'partitions to DROP']:
                if type(err.args[0]) is str and re.findall(x, err.args[0]):  # ex_exceptions 不再继续等待重做的例外
                    print(x, err.args[0], 'pass')
                    return False
            print(day_forpast(0, ss='%Y-%m-%d %H:%M:%S'), 'Exception:flag(%s):' % err_flag)
            print('   ', err, 'waiting for %s .... ' % self.wait, )
            time.sleep(self.wait)
            self.wait += 1
        else:
            self.quit('重连了太多次,aborted！')
        return boob

    def c_conn(self):  # change or create conn
        try:
            for k, v in self.default_dic.items():
                if k not in self.db_dic:
                    self.db_dic[k] = v
            self.conn = self.module.Connection(**self.db_dic)
            self.cur = self.conn.cursor()
            self.engine = self.engine_state % self.db_dic
            return self
        except Exception as err:
            if self.wait_for_('c_conn', err):
                return self.c_conn()

    def get_a_table(self, sql):
        if self.table == '':
            res_ss = re.findall('from\s+(\S+?)[\s;]', sql)
            if res_ss:
                self.table = res_ss[0]
        return self.table

    def do_single_sql(self, sql):
        rows = None
        if sql and len(sql) > 1:
            self.cur.execute(sql)
            self.conn.commit()
            try:
                rows = self.cur.fetchall()
            except:
                # print('fetchall error! ', err.args, end='', flush=True)
                rows = 0
        else:
            print('sql too short', end='', flush=True)
        return rows

    def _pre_do_sqls(self, sqls):
        # 长sql预处理,输入是长字符的sql,多段sql之间可以是;分割,处理前面的并返回最后一段sql
        sqls_li = sqls if type(sqls) is list else sqls.split(';')
        # 去掉结尾长度很小的无效sql
        while len(sqls_li) and len(re.sub('\s', '', sqls_li[-1])) < 6:
            sqls_li.pop(-1)
        for sql in sqls_li[:-1]:  #
            if len(re.sub('\s', '', sql)) > 6:
                _ = self.do_single_sql(sql)
        return sqls_li[-1] if len(sqls_li) else None

    def get_data(self, sqls):
        try:
            last_sql = self._pre_do_sqls(sqls)
            self.get_a_table(last_sql)
            return self.do_single_sql(last_sql)
        except Exception as err:
            if self.wait_for_('get_data', err):
                return self.c_conn().get_data(sqls)
            else:
                return None

    def to_dataframe(self, sqls):
        #  sqls   允许用;间隔多个sql,并可返回最后一个sql的返回结果去转换成dataframe
        try:
            last_sql = self._pre_do_sqls(sqls)
            self.get_a_table(last_sql)
            df = pd.read_sql(last_sql, self.conn)
            self.conn.commit()
            return df
        except Exception as err:
            if self.wait_for_('to_dataframe', err):
                return self.c_conn().to_dataframe(sqls)
            else:
                return pd.DataFrame([])

    def sql_engine(self, echo=False):
        return create_engine(self.engine, echo=echo, encoding='utf8')

    # 因为pyhive一个未解决的issue: https://github.com/dropbox/PyHive/issues?utf8=%E2%9C%93&q=to_sql 所以得造个简单轮子
    # 暂不考虑分区表
    def df_to_sql(self, df, db_table, cols_mat_rep_di=None, cols_comment_rep_di=None, force_replace_table=False):
        # 建表语句 cols_mat: 字段格式对照,默认string  cols_comment: 字段注释对照
        cols_mat = {x: 'string' for x in df.columns}
        if cols_mat_rep_di:
            cols_mat.update(cols_mat_rep_di)
        cols_comment = {x: '' for x in df.columns}
        if cols_comment_rep_di:
            cols_comment.update(cols_comment)
        cols_ss = ',\n'.join([f"{x} {cols_mat[x]} comment '{cols_comment[x]}'" for x in df.columns])
        create_ss = f'create table if not exists {db_table}({cols_ss})'
        if force_replace_table:
            create_ss = f'drop table if exists {db_table};\n' + create_ss
        self.get_data(create_ss)
        values = ','.join(['(%s)' % ','.join([f"'{df.loc[i, c]}'" for c in df.columns]) for i in df.index])
        insert_ss = f'INSERT INTO TABLE {db_table} VALUES {values}'
        return self.get_data(insert_ss)

    def quit(self, message=''):
        try:
            if message:
                print(message)
            # self.cur.close()
            self.conn.close()
        except Exception as err:
            print(day_forpast(0, ss='%Y-%m-%d %H:%M:%S'), 'Exception:flag(quit):', err.args[0])

