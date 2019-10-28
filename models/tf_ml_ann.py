
import numpy as np
import tensorflow as tf
# import tensorflow.compat.v1 as tf
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '1'
config1 = tf.compat.v1.ConfigProto()
# config1 = tf.ConfigProto()
config1.gpu_options.allow_growth = True
config1.allow_soft_placement = True
# config.gpu_options.per_process_gpu_memory_fraction = 0.02


def add_layer(inputs, out_size, activation_function=None, name='_', with_scope=False):
    # add one more layer and return the output of this layer
    # 可以加任意的层数 为DL打好基础
    in_size = int(inputs.shape[-1])  # inputs的最后一维
    if with_scope:
        # 大部件，定义层 layer，里面有 小部件 with定义的部件可以在tensorbord里看到
        with tf.name_scope('layer'):
            # 区别：小部件
            with tf.name_scope('weights'):
                weights = tf.Variable(tf.random_normal([in_size, out_size]), name='W'+name)
            with tf.name_scope('biases'):
                biases = tf.Variable(tf.zeros([1, out_size]) + 0.2, name='b'+name)
            # with tf.name_scope('wx_plus_b'):
    else:
        weights = tf.Variable(tf.random_normal([in_size, out_size]), name='W'+name)
        biases = tf.Variable(tf.zeros([1, out_size]) + 0.2, name='b'+name)

    wx_plus_b = tf.add(tf.matmul(inputs, weights), biases)
    if activation_function is None:
        outputs = wx_plus_b
    else:
        outputs = activation_function(wx_plus_b, name=name)
    return weights, biases, outputs


# L1正则化函数
def w11(weights):
    return tf.sqrt(tf.reduce_sum(tf.square(weights), 1))
def L1_loss(W):
    return tf.reduce_sum(w11(W))
def LN(W, n=0.5):  # https://www.zhihu.com/question/62605106
    return tf.reduce_sum(tf.pow(w11(W), n))


class ANNCon:

    def __init__(self, input_nodes, hidden_layers, lay_funcs, learning_rate=0.001, regu_func=None, name=''):
        """
        初始化network ie. hidden_layers=[2], lay_funcs=[tf.nn.sigmoid,None]
        :param input_nodes:
        :param hidden_layers: 列表分别代表每个隐藏层节点数 lay_funcs
        :param lay_funcs: 列表表示是每层激活函数 None 表示没有激活函数
        :param learning_rate:
        :param regu_func:  正则化函数
        :param name:
        """
        # self.name = ''.join([str(x) for x in ([input_nodes] + hidden_layers + [lay_funcs[-1].split('.')[-1]])])
        self.name = name
        self.hidden_layers_num = len(hidden_layers)
        self.init_flag = 0
        self.fit_times = 0
        with tf.name_scope('yinput'):
            self.y_in = tf.placeholder(tf.float32, [None, 1], name='y_input')

        cli = [input_nodes] + hidden_layers + [1]
        with tf.name_scope('xinput'):
            self.x_in = tf.placeholder(tf.float32, [None, cli[0]], name='x_input')

        self.all_layers = [self.x_in]  # all layers   input + hidden_layer + predict_layer
        self.layersW = []  # all layer weights
        self.layersb = []  # all layer biases
        for i in range(self.hidden_layers_num + 1):
            if isinstance(lay_funcs[i], str):
                lay_funcs[i] = eval(lay_funcs[i])
            w, b, o = add_layer(self.all_layers[-1], cli[i + 1], activation_function=lay_funcs[i], name=self.name)
            self.all_layers += [o]
            self.layersW += [w]
            self.layersb += [b]

        # Regularization 正则化约束 and loss
        with tf.name_scope('Regularization'):
            self.regu = eval(regu_func + '(self.layersW[0])') if regu_func else tf.constant(0, tf.float32)
        # with tf.device('/gpu:0'):
        with tf.name_scope('loss'):
            # reduction_indices是指沿tensor的哪些维度求和
            self.l1 = tf.reduce_mean(tf.abs(tf.subtract(self.all_layers[-1], self.y_in)), reduction_indices=[0])
            self.loss = self.l1 + self.regu

        # train
        # with tf.device('/gpu:0'):
        with tf.name_scope('train'):
            self.train_step = tf.train.AdamOptimizer(learning_rate=learning_rate).minimize(self.loss)
            # self.train_step = tf.train.MomentumOptimizer(learning_rate, momentum = 0.01).minimize(self.loss)
            # self.train_step = tf.train.GradientDescentOptimizer(learning_rate=0.1).minimize(self.loss)
        # tf sess init
        self.sess = tf.Session(config=config1)
        self.sess_init()
        self.x_data, self.y_data, self.yp_data = None, None, None
        self.x_norm_tp, self.y_norm_tp = '', ''
        self.x_delta_arr, self.y_delta_arr = None, None
        self.x_max_arr, self.y_max_arr = None, None
        self.length = 0
        self.mse = 9999
        self.test_ind = None
        self.train_ind = None
        # ANNCon的成员变量中需要被保存列表
        self.var_need_save_li = ['x_norm_tp', 'y_norm_tp', 'x_delta_arr', 'y_delta_arr', 'x_max_arr', 'y_max_arr']

    def sess_init(self, layers_w_in=None):
        if layers_w_in:
            for i in range(len(layers_w_in)):
                self.layersW[i] = tf.Variable(layers_w_in[i], name='W_'+self.name)
        self.sess.run(tf.global_variables_initializer())
        self.init_flag = 1
        return self

    def get_tf_value(self, val='self.layersW'):
        if type(val) is str:
            val = val if val[:4] == 'self' else 'self.'+val
            return self.sess.run(eval(val))
        else:
            return self.sess.run(val)

    def data_init(self, mat, test_size=1, y_col=-1):
        if y_col == -1:
            self.x_data = mat[:, :y_col]
        else:
            self.x_data = np.hstack((mat[:, :y_col], mat[:, y_col+1:]))
        self.y_data = mat[:, y_col:]
        self.yp_data = -mat[:, y_col:]  # 初始化一个全量y的输出值,后面会被改变

        self.length = len(self.y_data)
        if test_size > 0:
            return self.sieve_ind(test_size)
        else:
            return 'error input test_size!!!'

    def sieve_ind(self, sieve=0.2):
        if sieve >= 1:
            self.train_ind = list(range(0, self.length - int(sieve)))
            self.test_ind = list(range(self.length - int(sieve), self.length))
            return self
        else:
            p_arr_ = np.random.random(self.length)
            inx_p = p_arr_ < sieve
            inx_np = p_arr_ >= sieve
            self.test_ind = np.where(inx_p)[0].tolist()
            self.train_ind = np.where(inx_np)[0].tolist()
            if self.test_ind and self.train_ind:
                return self
            else:
                return self.sieve_ind(sieve)

    def pure_fit_with_be(self, be_ind_array, batch_size=100):  # be_ind_array 长度就是次数，数值就是每次起点索引
        # for be_ind in be_ind_array:  change to giant feed
        while be_ind_array.__len__():
            batch_arr, be_ind_array = be_ind_array[:batch_size], be_ind_array[batch_size:]
            x_training_data = np.vstack((self.x_data[self.train_ind, :][be_ind:, :] for be_ind in batch_arr))
            y_training_data = np.vstack((self.y_data[self.train_ind, :][be_ind:, :] for be_ind in batch_arr))
            self.sess.run(self.train_step, feed_dict={self.x_in: x_training_data, self.y_in: y_training_data})

    # 使用Cross-validation 当结束条件 一般用在测试集和验证集都很多时
    def nn_fit_with_cross(self, times=1000, batch_train_lp=0, op_err=0.00003, op_times=1, e_print=False, l_print=False):
        # batch_train_lp  批学习控制参数  此处是为了变向加重近期样本的权重（加强他们对学习结果的影响）.
        if self.train_ind.__len__() < 500 or self.test_ind.__len__() < 100:
            # print('sample_size(%s,%s) is too small !!' %(self.train_ind.__len__(),self.test_ind.__len__()))
            with_cross = False
        else:
            with_cross = True
        if self.fit_times == 0:
            self.sess_init()
        self.fit_times += 1
        self.mse = 9999
        standard_feed = {self.x_in: self.x_data[self.train_ind, :],
                         self.y_in: self.y_data[self.train_ind, :]}
        print_form = 'iter:%s||last_loss:%s||loss(%s)=train(%s)+cross(%s)||regu:%s||st:%s'
        i, st = 0, 0  # i:times计数, st:fit step out times计数
        mse_cross, mse_train, mse = 0.0, 0.0, 0.0
        for i in range(times):

            if with_cross:
                self.pure_fit_with_be([0, 0])
                mse_cross = self.sess.run(self.loss, feed_dict={self.x_in: self.x_data[self.test_ind, :],
                                                                self.y_in: self.y_data[self.test_ind, :]})
            else:
                arr_tmp = (np.random.random(99) - 1 / (batch_train_lp + 1)) * len(self.train_ind)
                be_ind_array = np.clip(arr_tmp, 0, len(self.train_ind) - 1).astype('int')
                self.pure_fit_with_be(be_ind_array, batch_size=40)

            mse_train = self.sess.run(self.loss, feed_dict=standard_feed)
            mse = mse_cross + mse_train
            delta_mse = mse - self.mse
            if op_err > delta_mse > -op_err:
                st += 1
            elif delta_mse > 10 * op_err:
                st += 2
            elif st > 0:
                st -= .5
            if e_print:
                print(print_form % (i, self.mse, mse, mse_train, mse_cross, '', st))

            self.mse = mse
            if st > op_times or mse < op_err * 100:
                break

        if l_print:
            regu_value = self.sess.run(self.regu, feed_dict=standard_feed)
            print(print_form % (i, self.mse, mse, mse_train, mse_cross, regu_value, st))
        return self

    def net_predict(self, do_inverse=True):
        # self.yp_data = self.sess.run(self.all_layers[-1], feed_dict={self.x_in: self.x_data})
        for i in self.test_ind:
            self.yp_data[i, :] = self.sess.run(self.all_layers[-1], feed_dict={self.x_in: self.x_data[[i], :]})
        if do_inverse:
            # TODO 这里可以有一个效能优化
            self.inverse_normalize()
        return self.yp_data

    def net_save_restore(self, save_path, global_step=1, sr_type='save'):
        # a_saver = tf.train.Saver(self.layersW + self.layersb)
        a_saver = tf.compat.v1.train.Saver(self.layersW + self.layersb)
        # a_saver = tf.train.Saver(self.layersW + self.layersb)

        if sr_type == 'save':
            a_saver.save(self.sess, save_path, global_step=global_step)
        elif sr_type == 'load_latest':
            model_file = tf.train.latest_checkpoint(save_path)
            a_saver.restore(self.sess, model_file)
        else:
            a_saver.restore(self.sess, save_path)

    def re_normalize_x(self):
        """
        再次因素归一化，要确保对因素之前是没做过的，一般在加载新的训练预测数据时使用，多用于online learning模式
        只包含重新计算self.x_data的过程，需要确保边界值是之前已经算过了，实际上它的用途正是不调整归一化边界的继续模式
        :return:
        """
        if self.x_norm_tp == 'dev_Max':
            self.x_data = self.x_data / self.x_max_arr
        elif self.x_norm_tp == 'rel_n11':
            self.x_data = (self.x_data - self.x_delta_arr) / self.x_max_arr

    def re_normalize_y(self):
        """
        再次因素归一化，要确保对因素之前是没做过的，一般在加载新的训练预测数据时使用，多用于online learning模式
        只包含重新计算self.y_data的过程，需要确保边界值是之前已经算过了，实际上它的用途正是不调整归一化边界的继续模式
        :return:
        """
        if self.y_norm_tp == 'dev_Max':
            self.y_data = self.y_data / self.y_max_arr
        elif self.y_norm_tp == 'rel_n11':
            self.y_data = (self.y_data - self.y_delta_arr) / self.y_max_arr

    def normalize_x(self, tp='dev_Max'):
        # self.x_norm_tp 也是标识状态 归一化过的就不再归一化
        if self.x_norm_tp and self.x_norm_tp == tp:
            return self

        if tp == 'dev_Max':
            self.x_max_arr = np.abs(self.x_data).max(0)
            if (self.x_max_arr == 0).any(): 
                self.x_max_arr[self.x_max_arr == 0] = 1
            # self.x_data = self.x_data / self.x_max_arr
        elif tp == 'rel_n11':
            self.x_delta_arr = self.x_data.min(0)
            self.x_max_arr = self.x_data.max(0) - self.x_delta_arr
            # self.x_data = (self.x_data - self.x_delta_arr) / self.x_max_arr
        self.x_norm_tp = tp
        self.re_normalize_x()
        return self

    def normalize_y(self, tp='dev_Max'):
        # self.y_norm_tp 也是标识状态 归一化过的就不再归一化
        if self.y_norm_tp and self.y_norm_tp == tp:
            return self

        if tp == 'dev_Max':
            self.y_max_arr = np.abs(self.y_data).max(0)
            if (self.y_max_arr == 0).any():
                self.y_max_arr[self.y_max_arr == 0] = 1
            # self.y_data = self.y_data / self.y_max_arr
        elif tp == 'rel_n11':
            self.y_max_arr = self.y_data.max(0) - self.y_data.min(0)
            self.y_delta_arr = self.y_data.min(0)
            # self.y_data = (self.y_data - self.y_delta_arr) / self.y_max_arr
        self.y_norm_tp = tp
        self.re_normalize_y()
        return self

    def instance_var2di(self, var_li=None):
        if not var_li:
            var_li = self.var_need_save_li
        return {x: self.__getattribute__(x) for x in var_li}

    def instance_dict2var(self, di_, inplace=True):
        if inplace:
            for k, v in di_.items():
                self.__setattr__(k, v)

    def inverse_normalize(self, only_y=True):
        if not only_y:
            if self.x_norm_tp == 'dev_Max':
                self.x_data = self.x_data * self.x_max_arr
            elif self.x_norm_tp == 'rel_n11':
                self.x_data = self.x_data * self.x_max_arr + self.x_delta_arr
            self.x_norm_tp = ''

        if self.y_norm_tp == 'dev_Max':
            self.y_data = self.y_data * self.y_max_arr
            self.yp_data *= self.y_max_arr
            self.y_norm_tp = None
        elif self.y_norm_tp == 'rel_n11':
            self.y_data = self.y_data * self.y_max_arr + self.y_delta_arr
            self.yp_data = self.yp_data * self.y_max_arr + self.y_delta_arr
            self.y_norm_tp = None
        self.y_norm_tp = ''
        return self

    # def __del__(self):
    #     try:
    #         self.sess.close()
    #     except Exception as err:
    #         print(' tensor_con.__del__: ', err)
