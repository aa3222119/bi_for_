from dao.__create_sqls import *
from dao.model_status_dao import *


def init_tables():
    my_bi_cli.sql_engine(sql_cr_bm_m2_u_behavior_profile1)
    my_bi_cli.sql_engine(sql_cr_bm_mx_model_status)

    # 初始化model status 1
    di_init = {'model_id': 1, 'record0': 'UNIX_TIMESTAMP("2013-01-01")', 'numb0': 1400, 'numb1': 0}
    replace_status_by_dict(di_init)

    for x in ['e', 'sma'] + list(range(1, 7)):
        my_bi_cli.sql_engine(sql_cr_bm_m1_up_predict_model_record_x % (x, x))

    my_bi_cli.sql_engine(sql_cr_bm_r1_up_predict_record)
    my_bi_cli.sql_engine(sql_cr_bm_r2_u_predict)
