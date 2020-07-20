"""
对应关系型数据库的helper
"""
from DbHelper.SqlBase import *
from settings import *  # 不同于NoSql的做法, 把账号变量放入helper内(而不是Base中), 由__init__定义导入了全部的setting文件

my_bi_cli = MysqlPyc(mysql_dbs[MYSQL_BI_NAME])  # 标准mysql_bi连接


def get_mysql_cli(name=MYSQL_BI_NAME):
    """
    返回一个连接对象
    :param name: mysql名称  默认：标准mysql_bi
    :return:
    """
    return MysqlPyc(mysql_dbs[name])


def get_pgsql_cli(name=PGSQL_BI_NAME):
    return PgsqlPyc(pgsql_dbs[name])


pg_bi_cli = get_pgsql_cli(PGSQL_BI_NAME)  # 标准pgsql连接


def get_table_one_line(table_name, my_cli=my_bi_cli, o_='df'):
    sql_s = f'select * from {table_name} limit 1'
    if o_ == 'df':
        return my_cli.to_dataframe(sql_s)
    else:
        return my_cli.sql_engine(sql_s)


def df_cols_cut_by_table(df, table_name):
    df_table = get_table_one_line(table_name)
    cols_cut = df.columns[list(map(lambda c: c in df_table.columns, df.columns))]
    return df.loc[:, cols_cut]


class SqlHandler:
    # sql特殊形式handler，定制一种特殊的更灵活的sql应用方式，目前依赖：
    # 1.sqls中含有多个group by目前只关注最后一个group by
    # 2.条件写法间具备唯一性

    def __init__(self, sqls, auto_anal=True):
        sqls = re.sub('\n', ' \n', sqls)
        self.sqls = sqls
        self.group_ss = ''
        self.group_res = ''
        self.group_list = []
        self.cont_list = []  # 条件列表(不包含_i_)
        self.cont_list_full = []   # 条件列表(包含_i_)
        exs = sqls.split("|")
        self.ex = exs[0]
        self.grb_ass = {}   # group by associate;
        if len(exs) > 1:
            try:
                self.grb_ass = eval(exs[1])     # groupby associate：被group by 牵扯的列，不能是最后一列，满足' %s,'
            except Exception as err:
                print(err)
        if auto_anal:
            self.get_sql_con().get_sql_group()

    def get_sql_con(self):
        cal_rex = re.compile(r"(\S+ \S+ )\S*_i_\S* ")
        cal_rex_full = re.compile(r"\S+ \S+ \S*_i_\S* ")
        self.cont_list = re.findall(cal_rex, self.sqls)
        self.cont_list_full = re.findall(cal_rex_full, self.sqls)
        for con, cona in zip(self.cont_list, self.cont_list_full):
            if con not in cona:
                print('cons check not pass! 这几乎不可能被打印出来')
        return self

    def get_sql_group(self):
        group_rex = re.compile(r"(group by )(\S+?)[;\s]")
        res = re.findall(group_rex, self.sqls)
        if res:
            self.group_res = res[-1]
            self.group_ss = self.group_res[0]
            self.group_list = self.group_res[1].split(",")
        return self

    def render_sqls(self, cond):
        res_ex = self.ex
        cond = cond.copy()
        group_para = cond.pop('group by ') if 'group by ' in cond.keys() else []
        full_cond = {x: '' for x in self.cont_list}
        full_cond.update(cond)
        cont_dict = {con: cona for con, cona in zip(self.cont_list, self.cont_list_full)}
        for ck, cv in full_cond.items():
            cv = str(cv)
            if ck in cont_dict:
                for x in ['and ', 'set ']:
                    # 用replace则可以完全抛弃add_transform函数
                    conx = '' if (cv == '') else x + cont_dict[ck].replace('_i_', cv)
                    res_ex = res_ex.replace(x + cont_dict[ck], conx)

        if group_para and self.group_ss:
            group_para_ = {x: '1' for x in self.group_list}
            group_para_.update(group_para)
            group_c = ','.join([gk for gk, gv in group_para.items() if gv == '1'])
            group_c = self.group_ss + group_c if group_c else ''
            ex = res_ex.replace(self.group_ss + self.group_res[1], group_c)
            for g in [gk for gk, gv in group_para_.items() if gv == '0']:
                if g in self.grb_ass:
                    ex = ex.replace(self.grb_ass[g], ' ')
                    for col in self.grb_ass[g].split(','):
                        ex = ex.replace(' %s,' % col, '')
        # self.render_ex = ex
        return res_ex
