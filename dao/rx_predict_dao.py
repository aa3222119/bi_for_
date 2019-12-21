from dao.m2_profile_dao import *

model_di = {i: f'{i}' for i in range(1, 5)}
model_di.update({0: 'e', -1: 'sma'})
# model_record_format = 'bm_m1_up_predict_model_record_%s'
model_record_format = "public.tbi_l6_mt_predict_model_record_%s_r"

r1_record_table = 'bm_r1_up_predict_record'
r2_predict_table = 'bm_r2_u_predict'


def di2my_ss(di_):
    """
    将字典转成mysql方便into的字符串
    :param di_:
    :return:
    """
    di_s = str(di_).replace('\'', '"')
    return f"'{di_s}'"


def err_rate1(yp, y_):
    """
    误差率
    :param yp: 预测值
    :param y_: 实际值
    :return:
    """
    return (yp - y_) / (yp + 0.01) if yp >= y_ else (y_ - yp) / (yp + 0.01)


# 修正的误差率
def err_rate2(yp, y_):
    """
    修正的误差率
    :param yp:
    :param y_:
    :return:
    """
    return (yp - y_) / (y_ + 0.01) if yp >= y_ else (y_ - yp) / (yp + 0.01)


def get_model_record_df(mid_, pay_time_='', con_c='=', table_=model_record_format % 'e'):
    """
    获取某用户已有的模型历史数据
    :param mid_:
    :param pay_time_:
    :param con_c: 需要条件的符号，比如 = > <  默认 =
    :param table_:
    :return:
    """
    pay_condition = f"and pay_time {con_c} '{pay_time_}'" if pay_time_ else ''
    # sql_ss = f"""
    # select hex(MID) as mid, pay_time, predict_type, predict_time, predict_dt, real_time, real_dt
    # from {table_}
    # where MID=x'{mid_}'
    # {pay_condition}
    # """
    # df_ = get_mysql_cli().to_dataframe(sql_ss)
    sql_ss = f"""
    select 
        encode(uuid_send(mid), 'hex') mid, pay_time, predict_type, predict_time, predict_dt, real_time, real_dt,
        model_para, model_choice
    from {table_} 
    where mid='{mid_}'
    {pay_condition}
    order by pay_time
    """
    df_ = get_pgsql_cli().to_dataframe(sql_ss)
    return df_


def get_evaluated_record_df(mid_, pay_time_='', con_c='=', table_=model_record_format % 'e'):
    """
        获取某用户已有的模型历史数据
        :param mid_:
        :param pay_time_:
        :param con_c: 需要条件的符号，比如 = > <  默认 =
        :param table_:
        :return:
    """
    pay_condition = f"and pay_time {con_c} '{pay_time_}'" if pay_time_ else ''
    sql_ss = f"""
        select  hex(MID) as mid, pay_time, real_time, err_rate1, err_rate2, d_err
        from {table_} 
        where MID = x'{mid_}' 
        {pay_condition}
        and d_err is not null
        """
    df_ = get_mysql_cli().to_dataframe(sql_ss)
    # df_ = df_.drop('MID', axis=1)
    return df_


def make_model_choice_insert(mid_, pay_time_s='', table_=model_record_format % 'e'):
    pay_time_s = f"'{pay_time_s}'" if '\'' not in pay_time_s else pay_time_s
    cols = 'MID, pay_time, predict_type, predict_time, predict_dt, p_accuracy, model_para, model_choice, obligate'
    sql_ss = f"""replace into {r2_predict_table}({cols}) 
    select {cols} 
    from {table_} 
    where mid = x'{mid_}'
    and pay_time = {pay_time_s}"""
    return get_mysql_cli().sql_engine(sql_ss)


def choice_a_predict(mid_):
    """

    :param mid_:
    :return:
    """
    # mid_ = '4ED0F5C6B88E4F87A3E7B0D901C0B1F4'
    # TODO 基于以前的历史
    get_model_record_df(mid_, table_=r1_record_table)
    min_err_avg = 20
    dfr_min, table_min = [], None
    for k_ in model_di:

        table_ = model_record_format % model_di[k_]
        dfrs = get_evaluated_record_df(mid_, table_=table_)
        # 进行各个模型误差汇总
        if len(dfrs):
            ser_ = dfrs.err_rate2.clip(0, 4) + dfrs.err_rate1.clip(0, 2) + dfrs.d_err.abs().clip(0, 4)
            err_di = {'evaluate_times': len(ser_), 'err_sum': ser_.sum()}
            err_di['err_avg'] = err_di['err_sum'] / err_di['evaluate_times']
            dfrs['model_choice'] = di2my_ss(err_di)
            dfr_ = dfrs.iloc[[-1]]
            # dfr_['model_choice'] = di2my_ss(err_di)
            for col in ['pay_time', 'real_time', ]:
                dfr_.loc[:, col] = dfr_[col].map(lambda x: f"'{x}'")
            general_upd_by_df(dfr_, table_)
            if err_di['err_avg'] < min_err_avg:
                print(err_di)
                dfr_min, table_min = dfr_, table_
                min_err_avg = err_di['err_avg']

    if len(dfr_min):
        print(make_model_choice_insert(mid_, dfr_min['real_time'].iloc[-1], table_=table_min))
    # TODO choice需要某用户第二次走入模型，给出一个第一次进入模型的结果体现在 r2_predict_table
    # TODO r1_record_table 的相关更新逻辑


def get_dfr_err_sum(dfr):
    return (dfr['err_rate2'].clip(0, 3) + dfr['r_err'].abs().clip(0, 3) + dfr['d_err'].abs().clip(0, 4)).sum()


def evaluate_dfr(dfr, real_time=None):
    """
    对model record 进行评价 默认需要dfr具备real_time
    :param dfr:
    :param real_time:  dfr可以依据传入的real_time
    :return:
    """
    if real_time:
        dfr.real_time = real_time
    dt_time = dfr.real_time - dfr.pay_time
    dfr.loc[:, 'real_dt'] = dt_time.map(lambda x: x.total_seconds()) / 86400
    real_day = dfr.real_time.map(lambda x: x.date())
    predict_day = dfr.predict_time.map(lambda x: x.date())
    # dfr['r_err'] = dfr['real_dt'] - dfr['predict_dt']
    # dfr['d_err'] = (real_day - predict_day).map(lambda x: x.days)
    # dfr['err_rate1'] = err_rate1(dfr['predict_dt'].iloc[-1], dfr['real_dt'].iloc[-1])
    # dfr['err_rate2'] = err_rate2(dfr['predict_dt'].iloc[-1], dfr['real_dt'].iloc[-1])
    dfr.insert(2, 'r_err', dfr['real_dt'] - dfr['predict_dt'])
    dfr.insert(2, 'd_err', (real_day - predict_day).map(lambda x: x.days))
    dfr.insert(2, 'err_rate1', err_rate1(dfr['predict_dt'].iloc[-1], dfr['real_dt'].iloc[-1]))
    dfr.insert(2, 'err_rate2', err_rate2(dfr['predict_dt'].iloc[-1], dfr['real_dt'].iloc[-1]))
    err_sum_ = get_dfr_err_sum(dfr)
    dfr.loc[:, 'model_para'] = str({'err_sum': err_sum_})
    dfr.loc[:, 'model_choice'] = str({'evaluate_times': 1, 'err_sum': err_sum_})
    return dfr


def df_update_record(df_):
    """
    通过 df_更新所有模型的反馈, df_需要含有 real_time
    :param df_:
    :return:
    """
    dfr_tobe_di = {k: [] for k, v in model_di.items()}
    min_err_di = {k: 99 for k, v in model_di.items()}
    choice_res_k, min_avg_err = 0, 99
    for k, v in model_di.items():
        # dfr_ = get_model_record_df(df_['mid'].iloc[-1], df_['pay_time'].iloc[-1], table_=table_)
        # # dfr_ = get_model_record_df('C312199A45544B46A3CADB6A8F79FC3B', '2018-05-07 16:52:40')
        # if len(dfr_) > 0:
        #     if pd.isna(dfr_['real_dt']).all():
        #         # dfr_ = evaluate_dfr(dfr_, df_['real_time'].iloc[-1])
        #         dfr_.real_time = df_['real_time'].iloc[-1]
        #         dfr_ = evaluate_dfr(dfr_)
        #         dfr_ = dfr_.drop('predict_time', axis=1)
        #         for col in ['pay_time', 'real_time', ]:
        #             dfr_[col] = dfr_[col].map(lambda x: f"'{x}'")
        #         general_upd_by_df(dfr_, table_)
        table_ = model_record_format % v
        dfr_ = get_model_record_df(df_['mid'].iloc[0], df_['pay_time'].iloc[0], con_c='>=', table_=table_)
        if len(dfr_) > 1:   # 不会大于3
            # print(dfr_)
            dfr_tobe = []
            if len(dfr_) == 2 and pd.isna(dfr_['real_dt'].iloc[0]):
                dfr_tobe = dfr_.iloc[0:1]
                dfr_tobe = evaluate_dfr(dfr_tobe, df_['real_time'].iloc[-1])
            elif len(dfr_) == 3 and pd.isna(dfr_['real_dt'].iloc[1]):
                dfr_tobe = dfr_.iloc[1:2]
                dfr_tobe = evaluate_dfr(dfr_tobe, df_['real_time'].iloc[-1])
                if not (dfr_['model_choice'].iloc[0] is None):  # 需要有历史才行
                    err_sum_di = eval(dfr_['model_choice'].iloc[0])
                    err_sum_di['evaluate_times'] += 1
                    err_sum_di['err_sum'] += eval(dfr_tobe.model_choice.iloc[0])['err_sum']
                    err_sum_di['err_avg'] = err_sum_di['err_sum'] / err_sum_di['evaluate_times']
                    min_err_di[k] = err_sum_di['err_avg']
                    dfr_tobe.loc[:, 'model_choice'] = str(err_sum_di)
            if len(dfr_tobe) == 1:
                dfr_tobe = dfr_tobe.drop('predict_time', axis=1)
                for col in ['pay_time', 'real_time', ]:
                    dfr_tobe[col] = dfr_tobe[col].map(lambda x: f"'{x}'")
                for col in ['model_para', 'model_choice']:
                    dfr_tobe[col] = dfr_tobe[col].map(di2my_ss)
                dfr_tobe_di[k] = dfr_tobe
                # 每个表根据粒度不同有自己的id取法
                dfr_tobe.loc[:, 'id'] = (dfr_tobe.mid + dfr_tobe.pay_time).map(lambda x: f"'{sha2str(x, hashlib.md5)}'")
                general_upd_by_df(dfr_tobe.iloc[0:1], table_)
    # 模型选择
    for k, v in min_err_di.items():
        if v < min_avg_err:
            choice_res_k, min_avg_err = k, v
    # 更新public.tbi_l7_m_predict_r
    # choice_table_ = model_record_format % model_di[choice_res_k]
    choice_dfr_ = dfr_tobe_di[choice_res_k]
    if len(choice_dfr_):

        choice_dfr_ = choice_dfr_.drop(['real_time', 'real_dt', 'r_err', 'd_err', 'err_rate1', 'err_rate2'], axis=1)
        # done 无论如何都先更新 public.tbi_l6_mt_predict_record_r
        choice_dfr_.loc[:, 'id'] = (choice_dfr_.mid + choice_dfr_.pay_time). \
            map(lambda x: f"'{sha2str(x, hashlib.md5)}'")
        general_upd_by_df(choice_dfr_, 'public.tbi_l6_mt_predict_record_r')
        # 更新tbi_l7_m_predict_r
        choice_dfr_.loc[:, 'id'] = choice_dfr_.mid.map(lambda x: f"'{sha2str(x, hashlib.md5)}'")
        general_upd_by_df(choice_dfr_, 'public.tbi_l7_m_predict_r')


def dfp_update_to_predict(df_):
    if not isinstance(df_['pay_time'].iloc[0], str):
        df_['pay_time'] = df_.pay_time.map(lambda x: f"'{x}'")
    predict_day = df_['pay_day'] + df_['predict_dt']
    # df_['predict_time'] = predict_day.map(lambda x: f'FROM_UNIXTIME({x * 86400})')
    df_['predict_time'] = predict_day.map(lambda x: f'to_timestamp({x * 86400})')
    # df_.drop('pay_day', axis=1, inplace=True)
    df_ = df_.drop('pay_day', axis=1, )
    df_.loc[:, 'id'] = (df_.mid + df_.pay_time).map(lambda x: f"'{sha2str(x, hashlib.md5)}'")
    return general_upd_by_df(df_, model_record_format % model_di[df_.predict_type.iloc[0]])

