from dao.m2_profile_dao import *

model_di = {i: f'{i}' for i in range(1, 5)}
model_di.update({0: 'e', -1: 'sma'})
model_record_format = 'bm_m1_up_predict_model_record_%s'

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
    pay_condition = f"and Pay_Time {con_c} '{pay_time_}'" if pay_time_ else ''
    sql_ss = f"""
    select hex(MID) as mid, Pay_Time, predict_type, predict_time, predict_dt, real_time, real_dt
    from {table_} 
    where MID=x'{mid_}' 
    {pay_condition}
    """
    df_ = get_mysql_cli().to_dataframe(sql_ss)
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
    pay_condition = f"and Pay_Time {con_c} '{pay_time_}'" if pay_time_ else ''
    sql_ss = f"""
        select  hex(MID) as mid, Pay_Time, real_time, err_rate1, err_rate2, d_err
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
    and Pay_Time = {pay_time_s}"""
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
            for col in ['Pay_Time', 'real_time', ]:
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


def evaluate_dfr(dfr, real_time=None):
    """
    对model record 进行评价 默认需要dfr具备real_time
    :param dfr:
    :param real_time:  dfr可以依据传入的real_time
    :return:
    """
    if real_time:
        dfr.real_time = real_time
    dt_time = dfr.real_time - dfr.Pay_Time.iloc[-1]
    dfr['real_dt'] = dt_time.map(lambda x: x.total_seconds()) / 86400
    real_day = dfr.real_time.map(lambda x: x.date())
    predict_day = dfr.predict_time.map(lambda x: x.date())
    dfr['r_err'] = dfr['real_dt'] - dfr['predict_dt']
    dfr['d_err'] = (real_day - predict_day).map(lambda x: x.days)
    dfr['err_rate1'] = err_rate1(dfr['predict_dt'].iloc[-1], dfr['real_dt'].iloc[-1])
    dfr['err_rate2'] = err_rate2(dfr['predict_dt'].iloc[-1], dfr['real_dt'].iloc[-1])
    return dfr


def df_update_record(df_):
    """
    通过 df_更新所有模型的反馈, df_需要含有 real_time
    :param df_:
    :return:
    """
    for table_ in [model_record_format % v for _, v in model_di.items()]:
        dfr_ = get_model_record_df(df_['mid'].iloc[-1], df_['Pay_Time'].iloc[-1], table_=table_)
        # dfr_ = get_model_record_df('C312199A45544B46A3CADB6A8F79FC3B', '2018-05-07 16:52:40')
        if len(dfr_) > 0:
            if pd.isna(dfr_['real_dt']).all():
                # dfr_ = evaluate_dfr(dfr_, df_['real_time'].iloc[-1])
                dfr_.real_time = df_['real_time'].iloc[-1]
                dfr_ = evaluate_dfr(dfr_)
                dfr_ = dfr_.drop('predict_time', axis=1)
                for col in ['Pay_Time', 'real_time', ]:
                    dfr_[col] = dfr_[col].map(lambda x: f"'{x}'")
                general_upd_by_df(dfr_, table_)


def dfp_update_to_predict(df_):
    if not isinstance(df_['Pay_Time'].iloc[0], str):
        df_['Pay_Time'] = df_.Pay_Time.map(lambda x: f"'{x}'")
    predict_day = df_['pay_day'] + df_['predict_dt']
    df_['predict_time'] = predict_day.map(lambda x: f'FROM_UNIXTIME({x * 86400})')
    # df_.drop('pay_day', axis=1, inplace=True)
    df_ = df_.drop('pay_day', axis=1, )
    general_upd_by_df(df_, model_record_format % model_di[df_.predict_type.iloc[0]])

