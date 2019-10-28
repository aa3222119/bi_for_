"""
    general math functions
"""


def nearest_in_li(li_, cen_num=0):
    """
    查找 列表li_中距离cen_num最近的元素
    :param li_:
    :param cen_num: 中心元素
    :return:
    """
    m_diff = 999999
    ret = None
    for ele in li_:
        diff_ = abs(ele - cen_num)
        if diff_ < m_diff:
            ret, m_diff = ele, diff_
    return ret


def round_units_to(num, digs='0,5,6,8'):
    """
    可以使用regress_units替代了
    :param num:
    :param digs:
    :return:
    """
    d_li = [int(x) for x in (digs + ',10').split(',')]
    num = int(num)
    tens, units = int(num / 10), num % 10
    units_new = nearest_in_li(d_li, units)
    return 10 * tens + units_new


def regress_units(num, digs='0,5,6,8', with_round=False):  # 将num的个位数划归到digs
    """
    将 num的个位数字划归到digs的元素上面，可以向下(default) 或者 最近
    :param num:
    :param digs: 允许的个位数合集
    :param with_round: 可以向下(default) 或者 最近(with_round=True)
    :return:
    """
    num = float(num)
    d_li = [int(x) for x in (digs + ',10').split(',')]
    tens, units = int(num / 10), num % 10
    if units in d_li[:-1]:
        return num
    if with_round:
        units = nearest_in_li(d_li, units)
    else:

        for i in range(len(d_li)-1):
            if d_li[i] < units < d_li[i+1]:
                units = d_li[i]
                break
    return 10 * tens + units


def k_avg(v1, v2, k=0.5):
    if v1 is None:
        return v2
    return (1 - k) * v1 + k * v2


def circle_avg(v1, v2, k=0.5, circle=24):
    # 类似clock, 在首尾相连的情况下求均值。如：23点和03点的均值是01点
    half_c = .5 * circle
    if abs(v1 - v2) < half_c:
        return k_avg(v1, v2, k)
    else:
        if v1 < v2:
            v1 += circle
        else:
            v2 += circle
        res_ = k_avg(v1, v2, k)
        return res_ if res_ < circle else res_ - circle


if __name__ == '__main__':
    circle_avg(23, 2)
    circle_avg(2, 6, 0.5, 7)
