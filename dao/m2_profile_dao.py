from dao.SqlHelper import *

m2_u_b_p1 = 'bm_m2_u_behavior_profile1'
l7_m_dp1 = 'public.tbi_l7_m_deed_profile1_r'


def l7_m_dp1_upd_by_df(df_):
    # df_['id'] = df_.mid.map(lambda x:  sha2str(x, hashlib.sha1))   # 直接一个字段就可以不用hash
    df_['id'] = df_.mid.map(lambda x: f"'{x}'")
    df_['mid'] = df_['id']
    pg_bi_cli.df_upd_tosql(df_, 2000, l7_m_dp1)


def get_l7_m_dp1(mid_):
    return pg_bi_cli.to_dataframe(f"select * from {l7_m_dp1} where mid = '{mid_}'")


def m2_p1_upd_by_df(df_):
    if 'mid' in df_.columns:
        df_['MID'] = df_.mid.map(lambda x:  f"x'{x}'")
    df_ = df_.drop('mid', 1)
    my_bi_cli.df_upd_tosql(df_, 2000, m2_u_b_p1)


def general_upd_by_df(df_, table_name):
    """
    任意中间结果表table_name的更新
    :param df_:
    :param table_name:
    :return:
    """
    # if 'mid' in df_.columns:
    #     df_.loc[:, 'MID'] = df_.mid.map(lambda x:  f"x'{x}'")
    # df_ = df_.drop('mid', 1)
    # return get_mysql_cli().df_upd_tosql(df_, 2000, table_name)
    # TODO 一下只针对了mid 其实其他类似这种字符字段有更通用的写法
    to_str_cols = ['mid']
    for c in to_str_cols:
        if c in df_.columns and df_[c].iloc[0][0] != "'":
            df_.loc[:, c] = df_.mid.map(lambda x: f"'{x}'")
    return get_pgsql_cli().df_upd_tosql(df_, 2000, table_name)


def get_m2_p1(mid_):
    return my_bi_cli.to_dataframe(f"select *,hex(MID) as mid from {m2_u_b_p1} where MID = x'{mid_}'")
