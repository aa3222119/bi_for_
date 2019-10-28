

def min_distance(word1, word2):
    if not word1:
        return len(word2 or '') or 0
    if not word2:
        return len(word1 or '') or 0
    size1 = len(word1)
    size2 = len(word2)
    tmp = list(range(size2 + 1))
    value = None
    for i in range(size1):
        tmp[0] = i + 1
        last = i
        for j in range(size2):
            if word1[i] == word2[j]:
                value = last
            else:
                value = 1 + min(last, tmp[j], tmp[j + 1])
            last = tmp[j+1]
            tmp[j+1] = value
    return value


def str_inx(word_, string_):
    return [i for i in range(len(string_)) if string_[i] == word_]


def ab_max_inx(s_a, s_b):
    i, len_a, len_b = 0, len(s_a), len(s_b)
    while len_a > i and len_b > i and s_a[i] == s_b[i]:
        i += 1
    return i


def common_substr(s_a, s_b):
    """
    两个字符串的所有公共子串，包含长度为1的
    :param s_a:
    :param s_b:
    :return:
    """
    res = []
    if s_a:
        a0_inx_in_b = str_inx(s_a[0], s_b)
        if a0_inx_in_b:
            b_end_inx, a_end_inx = -1, 0
            for inx in a0_inx_in_b:
                if b_end_inx > inx:
                    continue
                this_inx = ab_max_inx(s_a, s_b[inx:])
                a_end_inx = max(a_end_inx, this_inx)
                res.append(s_a[:this_inx])
                b_end_inx = this_inx + inx
            res += common_substr(s_a[a_end_inx:], s_b)
        else:
            res += common_substr(s_a[1:], s_b)
    return res


def max_common_substr(s_a, s_b):
    """
    两个字符串的最大公共子串，包含长度为1的
    :param s_a:
    :param s_b:
    :return:
    """
    common_li = common_substr(s_a, s_b)
    max_len = max([len(ss) for ss in common_li])
    for ss in common_li:
        if len(ss) == max_len:
            return ss
