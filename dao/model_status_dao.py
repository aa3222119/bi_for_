from dao.SqlHelper import *

model_status_table = 'bm_mx_model_status'


def get_status_by_mid(mid, o_type='df'):
    sql_ss = f'select *,FROM_UNIXTIME(record0) from {model_status_table} where model_id ={mid}'
    if o_type == 'df':
        return my_bi_cli.to_dataframe(sql_ss)
    else:
        return my_bi_cli.get_data(sql_ss)


def replace_status_by_dict(di_):
    # di_ = {'model_id': 1, 'record0': 1400023124, 'numb0': 140}
    return my_bi_cli.di_upd_tosql(di_, model_status_table)


def replace_status_by_df(df_):
    """
    更新的话要注意下面指定的列
    :param df_: status_df 输入
    :return:
    """
    # df_ = get_status_by_mid(1)
    # df_.loc[0, 'record0'] += 222222
    df_ = df_.loc[:, ['model_id', 'record0', 'numb0', 'numb1']]
    return my_bi_cli.df_upd_tosql(df_, table=model_status_table)


if __name__ == '__main__':
    # di_init = {'model_id': 1, 'record0': 'UNIX_TIMESTAMP("2011-01-01")', 'numb0': 1400}
    # replace_status_by_dict(di_init)
    pass
