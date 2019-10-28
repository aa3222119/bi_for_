from dao.SqlHelper import *

m2_u_b_p1 = 'bm_m2_u_behavior_profile1'


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
    if 'mid' in df_.columns:
        df_.loc[:, 'MID'] = df_.mid.map(lambda x:  f"x'{x}'")
    df_ = df_.drop('mid', 1)
    return get_mysql_cli().df_upd_tosql(df_, 2000, table_name)


def get_m2_p1(mid_):
    return my_bi_cli.to_dataframe(f"select *,hex(MID) as mid from {m2_u_b_p1} where MID = x'{mid_}'")
