from dao.orders_dao import *
from dao.m2_profile_dao import *


df = general_order_get(1546000000, 140000)
dfr = df.groupby('mid')['dp_all'].sum().reset_index()
dfr = dfr.rename(columns={'dp_all': 'coupon_price_sum'})

dfr['coupon_price_sum'] += 1
m2_p1_upd_by_df(dfr)

df1 = get_member_order_all('4F9DD005464E4E5D8FA30B4054D85229')

