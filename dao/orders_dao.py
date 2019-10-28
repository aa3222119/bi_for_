from dao.SqlHelper import *

order_table = 'tconsumption'
order_detail_table = 'tconsumption_detail'


general_order_sql = f"""
"""

order_cols = """hex(MID) as mid,Merchant_Code,Station_Code,Device_Code,Pay_Time,
    UNIX_TIMESTAMP(Pay_Time)/86400 pay_day,
    cast(TIME_FORMAT(Pay_Time,'%H') as UNSIGNED) + cast(TIME_FORMAT(Pay_Time,'%i') as UNSIGNED)/60 pay_toc,
    UNIX_TIMESTAMP(T.Pay_Time)-UNIX_TIMESTAMP(T.Trans_Time) pay_diff_s,
    Trans_Type,Data_Source,VIP_No,
    T.Original_Amount*100 op_all,T.Discounted_Amount*100 dp_all,
    TD.Original_Amount*100 op,TD.Discounted_Amount*100 dp,
    Rounding,`Status`,Item_No,Item_Trans_Type,`Type`='Y' is_oil,Quantity,
    DAYOFWEEK(Pay_Time) weekday,
    DAYOFMONTH(Pay_Time) monthday,
    DAYOFYEAR(Pay_Time) yd,
    if(`Type`='Y', 1, 0) oil_count,
    if(`Type`='Y', TD.Original_Amount*100, 0) oil_op,
    if(`Type`='Y', TD.Discounted_Amount*100, 0) oil_dp,
    if(`Type`='Y', Quantity, 0) oil_quantity,
    if(`Type`='Y', 0, 1) no_oil_count,
    if(`Type`='Y', 0, TD.Original_Amount*100) no_oil_op,
    if(`Type`='Y', 0, TD.Discounted_Amount*100) no_oil_dp,
    if(`Type`='Y', 0, Quantity) no_oil_quantity,
    1 order_times
"""


def general_order_get(time_start, during=140):
    sql_ss = f"""
    select {order_cols}
    from {order_table} T
    LEFT JOIN {order_detail_table} TD 
    ON T.ID = TD.Consumption_ID
    where Pay_Time BETWEEN FROM_UNIXTIME({time_start}) AND FROM_UNIXTIME({time_start + during})
    and MId is not NULL
    and T.Status = 100
    order by Pay_Time
    """
    return my_bi_cli.to_dataframe(sql_ss)


def get_member_order_all(mid_, end_time=None):
    end_condition = ''
    if end_time:
        end_condition = f'and Pay_Time < FROM_UNIXTIME({end_time})'
    sql_ss = f"""
        select {order_cols}
        from {order_table} T
        LEFT JOIN {order_detail_table} TD 
        ON T.ID = TD.Consumption_ID
        where MId = x'{mid_}'
        {end_condition}
        and T.Status = 100
        order by Pay_Time
    """
    # print(sql_ss)
    return get_mysql_cli().to_dataframe(sql_ss)


class MemberOrderCache:

    def __init__(self, df_):
        self.create_time = time.time()
        self.df = df_
        self.df_p = None  # 存储处理过待预测的df
        self.dfr = None  # 存储处理过待预测的df
        self.mid = self.df["mid"][0]
        self.update_time = time.time()
        self.max_pay_day = df_.pay_day.iloc[-1]
        # self.item_no = self.df.Item_No.to_list()
        self.len_ = 0
        self.max_dt = 99  # 一般处理的最大天数

    def update_by_one(self, df_row):
        if df_row['pay_day'] < self.max_pay_day:
            #  or (df_row['pay_day'] == self.max_pay_day and self.item_no == df_row['Item_No']):
            # print(f'\r{self.mid} already have this order <<', end='')
            print_wf(f'{self.mid} already have this order <<')
        else:
            # print(f'{self.mid} append -- 需要真实的生产环境才能测出这种情况，即模型赶上实时了且有订单流入')
            print(f'{self.mid}', len(self.df), self.max_pay_day, end=' append-->')
            self.df = self.df.append(df_row, ignore_index=True)
            self.max_pay_day = self.df.pay_day.iloc[-1]
            # self.item_no = self.df.Item_No.iloc[-1]
            print(len(self.df), self.max_pay_day)
        return self

    def update_by_rows(self, df_):
        df_i = df_.reset_index()
        #  太过偶现(好几个小时才会出现一次)的调试方式，不用try可以使其停下来
        if 'pay_day' not in df_i.columns:
            print(df_i)
            print(df_)
            print('mark 2 !!! ', df_i.columns, df_.columns)
        if df_i['pay_day'].iloc[-1] <= self.max_pay_day:
            # print(f'\r{self.mid} already have this order <<', end='')
            print_wf(f'{self.mid} already have this order <<')
        else:
            print(f'  {self.mid}', len(self.df), self.max_pay_day, end=' append-->')
            self.df = self.df.append(df_i, ignore_index=True)
            self.max_pay_day = self.df.pay_day.iloc[-1]
            print(len(self.df), self.max_pay_day)
        return self

    def update_df_pr(self, df_, dfr_):
        # 更新最需要的self.df_p，以及dfr_，写成方法比直接外围赋值要好
        self.df_p, self.dfr = df_, dfr_
        self.len_ = len(self.df_p)
        # 小于2两次的一般是0
        self.max_dt = self.dfr['diff_time_max_fixed'][0] if 'diff_time_max_fixed' in self.dfr else self.max_dt


# if __name__ == '__main__':
#     df = general_predict_order_get(1547078464, 1400)
#     dfr = df.groupby('mid')['Discounted_Amount'].sum().reset_index()
#     dfr = dfr.rename(columns={'Discounted_Amount': 'coupon_price_sum'})
#     from dao.m2_profile_dao import *
#
#     dfr['coupon_price_sum'] += 1
#     m2_p1_upd_by_df(dfr)

