from dao.orders_dao import *
from dao.rx_predict_dao import *
from dao.model_status_dao import *
from buildings.comm_across import *
from buildings.math_funcs import *
from buildings.ml_tro_funcs import *
from models.predict_model_paras import *
import threading

# 调试的时候把这里打开
# glo_orders_cache = {}


glo_variable = {'batch_orders_expect': 240,
                'orders_end_time': 140000000,  # 取个人全量时的时间上限
                'delay_secs': 1800,  # 实时系统延迟于现实的秒数
                'pursue_secs': 999999,  # 需要追赶的秒数,小于零时表示系统已经赶超了，需要等待的秒数
                'max_order_num': 0,  # 最大用户数的记录
                'center_network_ready': 0,  # 中心网络的ready状态，大于0时记录更新次数
                }


def insert_glo_cache(glo_orders_cache, mid_, with_end_time_limit=True, with_force=False):
    # 不强制以及本来就包含的时候则不更新
    if not with_force and mid_ in glo_orders_cache:
        return 0
    if with_end_time_limit:
        order_all_df = get_member_order_all(mid_, glo_variable['orders_end_time'])
    else:
        order_all_df = get_member_order_all(mid_)
    glo_orders_cache.update({mid_: MemberOrderCache(order_all_df)})


def get_handle_list():
    for _ in range(9999):
        status_df1 = get_status_by_mid(2)
        # 需要追赶的秒数,小于零时表示系统已经赶超了，需要等待的秒数
        glo_variable['pursue_secs'] = time.time() - glo_variable['delay_secs'] \
            - status_df1.loc[0, 'record0'] - status_df1.loc[0, 'numb0']
        Timer(0).runtime_delay(glo_variable['pursue_secs'])
        # 没有缓存的时候说明是第一次，会额外做-1的任务,回拨一下record0时间
        if glo_variable['max_order_num'] < 0:
            back_seconds = status_df1.loc[0, 'numb0'] * 1.5
            print(f'回拨时间{back_seconds} s...')
            status_df1.loc[0, 'record0'] -= back_seconds
        df_ = general_order_get(status_df1.loc[0, 'record0'], status_df1.loc[0, 'numb0'])
        status_df1.loc[0, 'record0'] += status_df1.loc[0, 'numb0']
        glo_variable['orders_end_time'] = status_df1.loc[0, 'record0']
        # 更新numb1(每次获取订单量的平滑平均值)，太低的时候增大numb0(每次获取多少秒的订单)
        # 这个调节还能反应出不同系统内的订单密度，无论多密集/稀疏的系统，每次处理的平滑单数能达到平衡
        status_df1.loc[0, 'numb1'] = k_avg(status_df1.loc[0, 'numb1'], len(df_), .6)
        # 依据numb1对numb0进行修正，旨在将numb1保持在glo_variable['batch_orders_expect']附近
        diff_ = glo_variable['batch_orders_expect'] - status_df1.loc[0, 'numb1']
        # diff_ / glo_variable['batch_orders_expect'] 的取值范围是-1到无穷
        # 取0时意为比较平衡的状态，此时diff为0， 2**0是1则numb0保持不变
        numb_new = status_df1.loc[0, 'numb0'] * (2 ** (diff_ / glo_variable['batch_orders_expect']))
        # numb0 也不能跑出这个范围太远，否则收不住
        if 99 < numb_new < 240000:
            status_df1.loc[0, 'numb0'] = numb_new
            # print(diff_ / glo_variable['batch_orders_expect'], status_df1.loc[0, 'numb0'])
        if status_df1.loc[0, 'record0'] < time.time():
            replace_status_by_df(status_df1)
        else:
            print('不能超现实时间，所以不更新')
            return df_
        if len(df_):
            return df_
        print(f'\r搜寻起始订单中{">" * (_ // 10 % 7)}：{_} {status_df1.loc[0, "record0"]}', end='')


def df_order_cd_p(df):
    """
    同单合并以及计算间隔
    :param df:
    :return:
    """
    df['dt'] = df['pay_day'].diff()[1:].reset_index()['pay_day']
    ind_too_close = df[df.dt < 0.05].index  # 0.005等于432秒 间隔太近的样本当做同一单
    # df['oil_count'], df['no_oil_count'] = 0, 0
    # df['oil_op'], df['no_oil_op'] = 0, 0
    # df['oil_dp'], df['no_oil_dp'] = 0, 0
    # df['oil_quantity'], df['no_oil_quantity'] = 0, 0
    add_cols = ['oil_times', 'no_oil_times', 'oil_op', 'no_oil_op', 'oil_dp', 'no_oil_dp',
                'oil_quantity', 'no_oil_quantity', 'order_times']
    # for c in add_cols:
    #     df[c] = 0
    # df['order_times'] = 1

    # inx1 = df.query('is_oil>0').index
    # df.loc[inx1, f'oil_count'] = 1
    # df.loc[inx1, f'oil_count'] = 1
    # inx2 = df.query('is_oil==0').index

    for i in ind_too_close:
        # prefix_s = 'oil_' if df.loc[i, 'is_oil'] > 0 else 'no_oil_'
        # df.loc[i, f'{prefix_s}count'] += 1
        # df.loc[i, f'{prefix_s}op'] += df.loc[i, 'op']
        # df.loc[i, f'{prefix_s}dp'] += df.loc[i, 'dp']
        # df.loc[i, f'{prefix_s}quantity'] += df.loc[i, 'Quantity']
        # 有间隔 只是间隔太近 则
        if df.loc[i, 'dt'] > 0:
            df.loc[i, 'order_times'] += 1
        df.loc[i + 1, add_cols] = df.loc[i, add_cols]

    # for c in ['op', 'dp', 'Item_No', 'Rounding', 'is_oil', 'Quantity']:
    #     df.drop(c, axis=1, inplace=True)
    df.drop(ind_too_close, inplace=True)
    df = df.fillna(df.dt.mean())  # 最后一个需要预测的位置先填上均值
    return df


def df_more_msa_p(df_more):
    """
    通过df的格式计算msa相关字段,输入为整个df后面一段的时候说明一般是用接过来更新
    :param df_more: 一定需要有长度
    :return:
    """
    fi_row = df_more.iloc[0]
    diff_time_, pay_toc_, weekday_, monthday_ = fi_row['dt'], fi_row['pay_toc'], fi_row['weekday'], fi_row['monthday']
    # 一次补充多条后面的单才会走入这里
    inx_max = df_more.iloc[1:].index.max()
    for inx, row_ in df_more.iloc[1:].iterrows():
        # diff_time_ 要少做最后那一次的
        if inx < inx_max:
            diff_time_ = k_avg(diff_time_, row_['dt'], .4)
        pay_toc_ = circle_avg(pay_toc_, row_['pay_toc'], .4)
        weekday_ = circle_avg(weekday_, row_['weekday'], .4, circle=7)
        monthday_ = circle_avg(monthday_, row_['monthday'], .4, circle=31)
    return diff_time_, pay_toc_, weekday_, monthday_


def fix_dt_p1(dt_, para1=28):
    """
    对dt列表进行fix的处理，一般方式
    :param dt_:
    :param para1: 均衡参数1
    :return:
    """
    dt_mean, dt_max_fixed = dt_.mean(), dt_.max()
    if dt_mean < para1 < dt_max_fixed:
        dt_max_fixed = k_avg(dt_mean, dt_max_fixed)
        dt_max_fixed = k_avg(dt_max_fixed, para1) if dt_max_fixed > para1 else dt_max_fixed
        dt_.loc[dt_ > dt_max_fixed] = dt_max_fixed
    return dt_max_fixed, dt_


def fix_dt_p2(dt_):
    """
    对dt列表进行fix的处理，考虑kmeans
    :param dt_:
    :return:
    """
    pass


def profile_ca(mid_, glo_orders_cache):
    """
    对一个会员的标准df进行画像处理
    :param mid_:
    :param glo_orders_cache:
    :return:
    """
    ca_ = glo_orders_cache[mid_]
    df = df_order_cd_p(ca_.df.copy())
    dfr = df.loc[:, ['mid', 'yd']].groupby('mid').count()
    dfr = dfr.rename(columns={'yd': 're_pay_count'})
    # 处理后所以dfr['re_pay_count'][0] 一定等于df_order_cd_p后df的长度
    dfr['order_count'] = df.order_times.sum()
    dfr['origin_amount_sum'] = df['op_all'].sum()
    dfr['pay_amount_sum'] = df['dp_all'].sum()

    dfr['fuel_count'] = df['oil_times'].sum()
    dfr['fuel_origin_amount'] = df['oil_op'].sum()
    dfr['fuel_pay_amount'] = df['oil_dp'].sum()
    dfr['fuel_quantity'] = df['oil_quantity'].sum()

    dfr['nonoil_count'] = df['no_oil_times'].sum()
    dfr['nonoil_origin_amount'] = df['no_oil_op'].sum()
    dfr['nonoil_pay_amount'] = df['no_oil_dp'].sum()
    dfr['nonoil_quantity'] = df['no_oil_quantity'].sum()

    # 目前对券的简化理解，实付和原价不同使用了优惠就算
    dfr['coupon_count'] = len(df.query('dp_all<op_all'))
    dfr['coupon_amount'] = (df['op_all'] - df['dp_all']).sum()

    # 处理后的df大于1次pay
    if dfr['re_pay_count'][0] > 1:
        dfr['origin_amount_std0'] = df['op_all'].std()
        dfr['pay_amount_std0'] = df['dp_all'].std()
        dfr['fuel_quantity_std0'] = df['oil_quantity'].std()
        dfr['diff_time_avg'] = df['dt'][:-1].mean()
        dfr['diff_time_max'] = df['dt'][:-1].max()
        dfr['diff_time_max_fixed'] = dfr['diff_time_max']
        # 新增的两个字段
        if len(df['dt'][:-1]) > 1:
            dfr['diff_time_std0'] = df['dt'][:-1].std()
            dfr['diff_time_std0_fixed'] = dfr['diff_time_std0']

        # 历史这个会员的画像
        # dfr_ord = get_m2_p1(dfr.index[0])  # if isinstance(cache_.dfr, type(None)) else cache_.dfr
        dfr_ord = get_l7_m_dp1(dfr.index[0])
        if len(dfr_ord) == 1:
            df_more = df.iloc[dfr_ord['re_pay_count'][0]:]
            # 已有了历史msa相关值
            if dfr_ord['diff_time_msa'][0] and len(df_more):
                diff_time_, pay_toc_, weekday_, monthday_ = df_more_msa_p(df_more)
                # print(dfr.index[0], '走入了msa修正', len(df_more), dfr_ord['diff_time_msa'][0], diff_time_)
                print_wf(f"{dfr.index[0]} 走入msa计算:{dfr_ord['diff_time_msa'][0]} to {diff_time_}, {len(df_more)}")
                dfr['diff_time_msa'] = k_avg(dfr_ord['diff_time_msa'][0], diff_time_, .4)
                dfr['pay_toc_msa'] = circle_avg(dfr_ord['pay_toc_msa'][0], pay_toc_, .4)
                dfr['weekday_msa'] = circle_avg(dfr_ord['weekday_msa'][0], weekday_, .4, circle=7)
                dfr['monthday_msa'] = circle_avg(dfr_ord['monthday_msa'][0], monthday_, .4, circle=31)
        # 保证re_pay_count大于1的一定要有msa那些
        if 'diff_time_msa' not in dfr.columns:
            dfr['diff_time_msa'], dfr['pay_toc_msa'], dfr['weekday_msa'], dfr['monthday_msa'] = df_more_msa_p(df)
            # print(dfr.index[0], '初始化diff_time_msa', dfr['diff_time_msa'].iloc[0])
            print_wf(f"{dfr.index[0]} 初始化diff_time_msa, {len(df)}，值{dfr['diff_time_msa'][0]}")
    # 考虑fix dt
    if dfr['re_pay_count'][0] > 6:
        dfr['diff_time_max_fixed'], df.dt = fix_dt_p1(df.dt)
        if dfr['diff_time_max_fixed'][0] < dfr['diff_time_max'][0]:
            print(f"diff_time_max_fixed 生效: {dfr['diff_time_max'][0]} fixed to {dfr['diff_time_max_fixed'][0]}")
            dfr['diff_time_std0_fixed'] = df['dt'][:-1].std()

    # m2_p1_upd_by_df(dfr.reset_index())
    l7_m_dp1_upd_by_df(dfr.reset_index())
    ca_.update_df_pr(df, dfr)
    glo_orders_cache.update({mid_: ca_})
    # glo_orders_cache[mid_].update_df_pr(df, dfr)
    # print(id(glo_orders_cache))
    # 其实此时的df是更新到glo_orders_cache[mid_].df_p 了 ，成员变量glo_orders_cache[mid_].df还是最初的
    return df, dfr


def set_ca_list(glo_orders_cache):
    """
    返回待处理用户id列表，并更新glo_orders_cache
    :param glo_orders_cache:
    :return:
    """

    handle_df = get_handle_list()
    ret_li = []
    if len(handle_df):
        df_mul_inx = handle_df.set_index(['mid', 'pay_day'])
        for mid_, pay_day_ in df_mul_inx.index.unique():
            if mid_ not in glo_orders_cache:
                # order_all_df = get_member_order_all(mid_, glo_variable['orders_end_time'])
                # glo_orders_cache[mid_] = MemberOrderCache(order_all_df)
                insert_glo_cache(glo_orders_cache, mid_)
                print_wf(f'{mid_} init~, insert all orders <<')
            else:
                ca_ = glo_orders_cache[mid_]
                ca_.update_by_rows(df_mul_inx.loc[[(mid_, pay_day_)], :])
                glo_orders_cache.update({mid_: ca_})

            if len(glo_orders_cache[mid_].df) > glo_variable['max_order_num']:
                df = glo_orders_cache[mid_].df
                glo_variable['max_order_num'] = len(df)
        # TODO glo_orders_cache 内存管理，内存占用过大时删除不热的用户
        print(f'****缓存的用户数{glo_orders_cache.__len__()}，glo_variable::', glo_variable)
        ret_li = handle_df.mid.unique().tolist()
    return ret_li


def df_mid_p(df_, id_cols_count=3):
    """
    对 mid字段进行处理，生成id_cols_count列id_x字段，每个id_x是4位16进制对应的数字
    :param df_:
    :param id_cols_count:
    :return:
    """
    if 'mid' in df_.columns:
        for i in range(id_cols_count):
            df_.insert(0, f'id_{i}', df_.mid.map(lambda x: int(x[4 * i: 4 * (i+1)], 16)))
    return df_


def mx_df_cut(df_, m_name='ann1'):
    """
    模型的df修剪
    :param df_:
    :param m_name:
    :return:
    """
    df = df_[glo_nn_raw_cols[m_name]]
    df = df_mid_p(df).drop('mid', 1)
    arr_ = np.array(df)
    # 阶梯参数
    if gol_nn_re_last_dts[m_name] > 0:
        arr_ = nth_ladder_create(arr_, gol_nn_re_last_dts[m_name])
    return arr_


def update_center_network(glo_orders_cache):
    print(f'glo_orders_cache({len(glo_orders_cache)})，', end=' >> ')
    if len(glo_orders_cache) < 100:
        print(' 太少，暂不进行update_center_network')
        return -1
    mx_arr_di = {}
    for mid_ in glo_orders_cache.keys():
        ca_ = glo_orders_cache[mid_]
        once_p = random.random()
        # print(once_p, sigmoid(np.log10(ca_.len_ + .1) + 1), ca_.len_)
        # TODO 这里还可以控制下避免使用到最近一小时才更新的，
        if once_p < sigmoid(np.log10(ca_.len_ + .1) + 1):
            
            for m_name in glo_nn_opt.keys():
                if ca_.df_p is None:
                    continue
                arr_ = mx_df_cut(ca_.df_p, m_name)
                # 每个用户 通过自己的max_dt 来归一化
                arr_[:, -1] /= ca_.max_dt
                if m_name not in mx_arr_di:
                    mx_arr_di[m_name] = arr_[:-1, :]
                else:
                    mx_arr_di[m_name] = np.vstack((mx_arr_di[m_name], arr_[:-1, :]))

    for m_name in glo_nn_opt.keys():
        samples_ = 0 if m_name not in mx_arr_di else mx_arr_di[m_name].shape[0]
        if samples_ < 1000:
            print(f'{m_name}，可用于训练中心的样本数应大于1000实际{samples_}', end=' >> ')
            continue
        glo_nn_opt[m_name]['input_nodes'] = mx_arr_di[m_name].shape[1] - 1
        save_dir = get_save_dir(m_name)

        my_nn = ANNCon(name=m_name, **glo_nn_opt[m_name])
        my_nn.data_init(mx_arr_di[m_name])
        my_nn.normalize_x(tp='rel_n11')
        # my_nn.normalize_y(tp='rel_n11')
        op_times = 5
        try:
            my_nn.net_save_restore(save_dir, sr_type='load_latest')
            print(f'{m_name}load success, 走到这里了才说明中心网络也有了online learning特性')
        except Exception as err:
            op_times = 10
            print(f'mark3,{err},op_times={op_times}', )

        my_nn.nn_fit_with_cross(op_times=op_times, l_print=True)

        global_step = int(str(samples_) + day_forpast(ss="%Y%m%d%H%M"))
        try:
            my_nn.net_save_restore(save_dir, global_step)
        except Exception as err:
            print(err)
            iter_mkdir(save_dir)
            my_nn.net_save_restore(save_dir, global_step)

        VHolder(my_nn.instance_var2di()).store(save_dir + 'nn_instance_saved_value')

        # 读取时
        # di_ = VHolder().pickup(save_dir + 'nn_instance_saved_value')
        # my_nn.instance_dict2var(di_)

        # my_nn.get_tf_value('layersW[2]')


def do_a_predict(ca_,):
    """
    对一个用户进行预测
    :param ca_:
    :return:
    """
    # mid_ = 'c6d5a8a515fb4603ae2c685d90aff0e3'
    # ca_ = glo_orders_cache[mid_]
    print(f'{ca_.mid} len:{ca_.len_}', end=' >>')
    if ca_.len_ < 2:
        print(' out ')
        return 0
    # 传统预测 均值 和 msa
    df_to_table = ca_.df_p.iloc[[-1]][['mid', 'pay_time', 'pay_day']]
    df_to_table['predict_type'] = 0
    df_to_table['predict_dt'] = ca_.dfr['diff_time_avg'][0]
    dfp_update_to_predict(df_to_table)

    df_to_table['predict_type'] = -1
    # print(ca_.dfr)
    try:
        df_to_table['predict_dt'] = ca_.dfr['diff_time_msa'][0]
    except Exception as err:
        print('mark 1 ！！！ ', err)
        print(ca_.dfr)
    dfp_update_to_predict(df_to_table)

    # 小于10次的则暂时不走入网络预测
    if ca_.len_ > 10:
        for m_name in glo_nn_opt.keys():
            arr_ = mx_df_cut(ca_.df_p, m_name)
            # 每个用户 通过自己的max_dt 来归一化
            # arr_[:, -1] /= ca_.max_dt
            glo_nn_opt[m_name]['input_nodes'] = arr_.shape[1] - 1
            my_nn = ANNCon(name=m_name, **glo_nn_opt[m_name])
            # 准备读取
            save_dir = get_save_dir(m_name)
            try:
                di_ = VHolder().pickup(save_dir + 'nn_instance_saved_value')
                # 因为从群体到个体，y的归一化放入my_nn内，存储的nn_instance_saved_value不包含y的归一化方式
                di_.update({'y_norm_tp': 'dev_Max', 'y_max_arr': ca_.max_dt})
                # 读取归一化方式
                my_nn.instance_dict2var(di_)
                # 读取网络
                my_nn.net_save_restore(save_dir, sr_type='load_latest')
            except AttributeError as err:
                print('flag:: 中心参数不存在', err)
            my_nn.data_init(arr_)
            my_nn.re_normalize_x()
            my_nn.re_normalize_y()
            my_nn.nn_fit_with_cross(times=300, op_err=0.0003, )  # e_print=True
            my_nn.net_predict(do_inverse=True)

            # 更新相应模型结果
            df_to_table['predict_type'] = gol_nn_ids[m_name]
            df_to_table['predict_dt'] = my_nn.yp_data[-1, 0]
            dfp_update_to_predict(df_to_table)

    # 更新历史record
    # do_num = 10 if ca_.len_ > 10 else ca_.len_ - 1
    do_num = 2 if ca_.len_ > 3 else 1
    dfr_record = ca_.df_p.iloc[-do_num - 1:-1][['mid', 'pay_time', 'pay_day']].reset_index(drop=True)
    dfr_record['real_time'] = ca_.df_p.iloc[-do_num:]['pay_time'].reset_index(drop=True)
    # done 加速方式
    # for inx in dfr_record.index:
    #     df_update_record(dfr_record.loc[[inx]])
    df_update_record(dfr_record)
    print(f'{ca_.mid} len:{ca_.len_} 完成所有子模型结果更新。 dfr_record {len(dfr_record)}')


def threading_hold_print(stack_to, wait_to, sleept=5, flush_ss=' '*140+'stall waiting for: %s threads .... \r'):
    # console_func: console auxiliary
    #  stack_to 开始堵塞的进程数; wait_to 等到wait_to的进程数解除锁定 ; sleept
    if threading.activeCount() >= stack_to:
        while threading.activeCount() > wait_to + 1:
            time.sleep(sleept)
            if len(flush_ss) > 0:
                sys.stdout.write(flush_ss % (threading.activeCount()-1))
                sys.stdout.flush()


def do_predict_(ca_li, ):
    # done predict过程不会改写glo_orders_cache 所以不需要这么重的存储共享
    task_li = []
    for ca_ in ca_li:
        # do_a_predict(mid_)
        task = threading.Thread(target=do_a_predict, args=(ca_, ))
        task.start()
        task_li.append(task)
        threading_hold_print(7, 3, sleept=5, flush_ss='fthreads num: %s .. \r')
    for task in task_li:
        task.join()


def do_choice_(mid_li,):
    for mid_ in mid_li:
        choice_a_predict(mid_)
