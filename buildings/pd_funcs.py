import pandas as pd


def group_count_join(df, cols):
    """
    分别按cols列表的每个字段c，去统计df按c分组的每组个数len，并join回原df
    :param df: 输入的df
    :param cols: 需要计算的字段
    :return: df+每个字段的len字段统计
    """
    for c in cols:
        dfg = pd.pivot_table(df, index=[c], aggfunc={c: len})
        c_len = c + '_len'
        dfg.rename(columns={c: c_len}, inplace=True)
        inx = df.index
        df = pd.merge(df, dfg.reset_index()[[c, c_len]], on=c, how='left')  # , left_index=True
        # df = df.join(dfg.reset_index()[[c, c_len]], on=c, how='left', rsuffix='_r')
        df.index = inx
    return df
