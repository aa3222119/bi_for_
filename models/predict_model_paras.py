from models.tf_ml_ann import *

glo_nn_opt = {
    'ann1': {'hidden_layers': [16, 12], 'lay_funcs': [None, tf.nn.tanh, tf.nn.sigmoid], 'regu_func': None},
    'ann2': {'hidden_layers': [24, 16], 'lay_funcs': [None, tf.nn.tanh, tf.nn.sigmoid], 'regu_func': None},
    'rnn1': {'hidden_layers': [12, 12], 'lay_funcs': [None, tf.nn.tanh, tf.nn.sigmoid], 'regu_func': None},
    'rnn2': {'hidden_layers': [14, 14], 'lay_funcs': [None, tf.nn.tanh, tf.nn.sigmoid], 'regu_func': None}, }
# 单用户构成的序列因素个数控制
gol_nn_re_last_dts = {'ann1': 0, 'ann2': 0, 'rnn1': 2, 'rnn2': 5}
# 模型与id对应，会直接影响进入的结果表
gol_nn_ids = {'ann1': 1, 'ann2': 2, 'rnn1': 3, 'rnn2': 4}

# ann1_cols = ['mid', 'pay_toc', 'op_all', 'dp_all', 'weekday', 'monthday', 'yd', 'dt']
# ann2_cols = ['mid', 'pay_toc', 'op_all', 'dp_all', 'weekday', 'monthday', 'yd',
#              'no_oil_count', 'oil_dp', 'oil_quantity', 'dt']
# rnn1_cols = ['mid', 'pay_toc', 'op_all', 'dp_all', 'weekday', 'monthday', 'yd',
#              'no_oil_count', 'oil_dp', 'oil_quantity', 'dt']
# rnn2_cols = ['mid', 'pay_toc', 'op_all', 'dp_all', 'weekday', 'monthday', 'yd',
#              'no_oil_count', 'oil_dp', 'oil_quantity', 'dt']
# glo_nn_raw_cols = {k: eval(f'{k}_cols') for k in glo_nn_opt.keys()}
glo_nn_raw_cols = {
    'ann1': ['mid', 'pay_toc', 'op_all', 'dp_all', 'weekday', 'monthday', 'yd', 'dt'],
    'ann2': ['mid', 'pay_toc', 'op_all', 'dp_all', 'weekday', 'monthday', 'yd',
             'no_oil_times', 'oil_dp', 'oil_quantity', 'dt'],
    'rnn1': ['mid', 'pay_toc', 'op_all', 'dp_all', 'weekday', 'monthday', 'yd',
             'no_oil_times', 'oil_dp', 'oil_quantity', 'dt'],
    'rnn2': ['mid', 'pay_toc', 'op_all', 'dp_all', 'weekday', 'monthday', 'yd',
             'no_oil_times', 'oil_dp', 'oil_quantity', 'dt']
}

# 有更准的方式
# for k in glo_nn_opt.keys():
#     glo_nn_opt[k]['input_nodes'] = glo_nn_raw_cols[k].__len__() - 1 + gol_nn_re_last_dts[k]

# glo_nn_names = ['ann1', 'ann2', 'rnn1', 'rnn2']


def get_save_dir(m_name='ann1', mer_id=''):
    nodes_ss = f"{glo_nn_opt[m_name]['input_nodes']}_" + '_'.join(map(str, glo_nn_opt[m_name]['hidden_layers']))
    return f".data/model_data2_{mer_id}/{m_name}/{nodes_ss}/Re_{gol_nn_re_last_dts[m_name]}/"
