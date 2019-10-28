# ===========================================
# @Time    : 2019/7/9 10:39
# @project : general
# @Author  : antony
# @Email   : 502202879@qq.com
# @File    : __init__.py
# @Software: PyCharm
# ===========================================
# 一些基于随机数的方法，碰运气或许是人类行动的本质，从很多看似随机产生的候选中找到符合自己价值观的路线
# 所以也可能是搜索相关算法的基石
# 用的不错可以大幅提升搜索速度
import random


def nd_inx_li(nd_shape):
    """
    产生任意维度的shape下的所有索引的组合
    :param nd_shape: 维度信息 如 (5,4)
    :return: 输出为所有索引组合的列表，可直接迭代，列表元素为tuple，每个tuple的长度等于维度数
    """
    iter_li = ['for x%s in range(%s)' % (ix, nd_shape[ix]) for ix in range(len(nd_shape))]
    iter_ss = ' '.join(iter_li)
    tuple_ss = '(%s,)' % ','.join(['x%s' % ix for ix in range(len(nd_shape))])
    return eval('[%s %s]' % (tuple_ss, iter_ss))  # pix 索引的-1维度的位置


def random_arrange_nd(nd_shape, length=10, with_playback=False):
    """
    任意维度所有排列的随机抽取输出，注意不是组合
    :param nd_shape: 维度信息 如 (5,4)
    :param length:  抽取次数
    :param with_playback: 是否有放回
    :return: 和 nd_inx_li的输出格式一致
    """
    inx_li = nd_inx_li(nd_shape)
    if with_playback:
        return [random.choice(inx_li) for _ in range(length)]
    else:
        random.shuffle(inx_li)
        return inx_li[:length]


def randint_ab(a=0, b=9):
    return random.randint(a, b)


def divided_list(obj_li, num=10, random_flag=0):
    """
    将一个列表分成num份，可采取随机数目，random_flag 为0时为均匀去分
    :param obj_li: 待分解的列表
    :param num: 分的个数, random_flag > 0时可能有不同含义
    :param random_flag:  为0时为均匀去分 大于零为不同的随机分模式
    :return:
    """
    res_li = []
    if random_flag == 0:
        list_len = len(obj_li)
        step_len = int(list_len/num) + (1 if list_len % num else 0)
        rest_li = obj_li
        while rest_li:
            res_li += [rest_li[:step_len]]
            rest_li = rest_li[step_len:]
    elif num > random_flag > 0:
        while obj_li:
            ind_x = round(randint_ab(random_flag, num))
            res_li += [obj_li[:ind_x]]
            obj_li = obj_li[ind_x:]
    else:
        print('have not done yet. ~.~')
    return res_li
