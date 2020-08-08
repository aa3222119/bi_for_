
from models.profile_predict import *
from models.init_env import *
from buildings.comm_across import *
from buildings.processing_funcs import *
from buildings.unstable_funcs import *

# 调试时启用
# glo_orders_cache = {}

mgr = multiprocessing.Manager()
glo_orders_cache = mgr.dict()


def mid2cha_li(mid_li, glo_cache_=glo_orders_cache):
    ret_li = []
    for mid_ in mid_li:
        if mid_ not in glo_cache_:
            # order_all_df = get_member_order_all(mid_)
            # glo_orders_cache[mid_] = MemberOrderCache(order_all_df)
            print(f'> {mid_} >>重新读取完毕member_order_all,')
            insert_glo_cache(glo_cache_, mid_)
        ret_li.append(glo_cache_[mid_])
    return ret_li


def user_profile_predict(with_init=False):
    """
    行为画像和消费预测主进程
    :param with_init:
    :return:
    """
    # 启动 update_center_network
    # general_bgp 由于是由一个进程不断sleep再执行，会引起tf的变量问题，需要每次update的时候都是新的子进程，general_bgp2可以满足
    if with_init:
        init_tables_pg()
    general_bgp2(update_center_network, (glo_orders_cache,), dt=4000)

    for i in range(999999):
        # 获取部分
        mid_li = set_ca_list(glo_orders_cache, with_init=with_init)
        # 画像部分
        for mid_ in mid_li:
            profile_ca(mid_, glo_orders_cache)
        # 预测和选择部分
        re_mid_li = divided_list(mid_li)
        for part_mid_li in re_mid_li:
            print(f'{i}, all_num:{len(mid_li)}, all_batch:{len(re_mid_li)}, this: {len(part_mid_li)}')
            ca_li = mid2cha_li(part_mid_li, )
            do_predict_(ca_li, )
            # do_choice_(part_mid_li)


if __name__ == "__main__":
    e_str = general_console([test_b1, user_profile_predict, init_tables])
    if e_str:
        eval(e_str)

    # test_b1(glo_orders_cache)
