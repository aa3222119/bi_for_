"""
    这里存储mysql建表语句
"""

# bi_source

sql_cr_bm_m2_u_behavior_profile1 = """
    create table IF NOT EXISTS bm_m2_u_behavior_profile1(
    `MID` binary(16) not null,
    re_pay_count int(11) DEFAULT 0 COMMENT '用户有效加油次数',
    order_count int(11) DEFAULT 0 COMMENT '用户所有订单数',
    origin_price_sum int(11) DEFAULT 0 COMMENT '总应付金额，单位：分',
    pay_price_sum int(11) DEFAULT 0 COMMENT '总付款金额，单位：分',
    origin_price_std0 double DEFAULT NULL COMMENT '应付金额标准差',
    pay_price_std0 double DEFAULT NULL COMMENT '付款金额金额标准差',
    oil_origin_price_sum int(11) DEFAULT 0 COMMENT '油品总应付金额，单位：分',
    oil_pay_price_sum int(11) DEFAULT 0 COMMENT '油品总付款金额，单位：分',
    oil_count int(11) DEFAULT 0 COMMENT '用户所有油品订单数',
    oil_liters_sum double DEFAULT 0 COMMENT '总加油升数',
    liters_std0 double DEFAULT NULL comment '加油升数标准差',
    no_oil_origin_price_sum int(11) DEFAULT 0 COMMENT '非油品总应付金额，单位：分',
    no_oil_pay_price_sum int(11) DEFAULT 0 COMMENT '非油品总付款金额，单位：分',
    no_oil_count int(11) DEFAULT 0 COMMENT '用户所有非油品订单数',
    no_oil_quantity_sum double DEFAULT 0 COMMENT '总非油品购买数',
    coupon_count int(11) DEFAULT 0 comment '总用券次数',
    coupon_price_sum int(11) DEFAULT 0 comment '总用券金额，单位：分',
    diff_time_avg double DEFAULT NULL COMMENT '间隔时间：平均值，单位天，后diff_time相关均为天',
    diff_time_msa double DEFAULT NULL COMMENT '间隔时间：移动平滑均值',
    pay_toc_msa double DEFAULT NULL COMMENT '付款时间点(如12:36分=12.60)：移动平滑均值',
    weekday_msa double DEFAULT NULL COMMENT '付款所在周几(0-6.999)：移动平滑均值',
    monthday_msa double DEFAULT NULL COMMENT '付款所在月的第几天：移动平滑均值',
    diff_time_max double DEFAULT NULL COMMENT '间隔时间最大值',
    diff_time_max_fixed double DEFAULT NULL COMMENT '间隔时间最大值，通过一些异常大的修正处理之后',
    obligate text DEFAULT NULL COMMENT '扩展',
    `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_time` timestamp DEFAULT '1990-01-01 00:00:00' ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY `idx_MID` (`MID`),
    KEY `IDX_UPD_TIME` (`update_time`) USING BTREE
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='中间表：用户行为画像表1(消费预测用)';
"""
    
sql_cr_bm_mx_model_status = """
create table IF NOT EXISTS bm_mx_model_status (
    model_id int(11) DEFAULT 0 COMMENT 'model_id',
    model_info text DEFAULT NULL COMMENT 'model描述',
    numb0 double DEFAULT NULL COMMENT '',
    numb1 double DEFAULT NULL COMMENT '',
    numb2 double DEFAULT NULL COMMENT '',
    record0 int(14) DEFAULT NULL COMMENT '',
    record1 int(14) DEFAULT NULL COMMENT '',
    record2 int(14) DEFAULT NULL COMMENT '',
    obligate text DEFAULT NULL COMMENT '扩展',
    `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_time` timestamp DEFAULT '1990-01-01 00:00:00' ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY `IDX_model_id` (`model_id`),
    KEY `IDX_UPD_TIME` (`update_time`) USING BTREE
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='中间表(),模型状态';
"""


sql_cr_bm_r2_u_predict = '''
create table IF NOT EXISTS bm_r2_u_predict (
    `MID` binary(16) not null,
    pay_time datetime COMMENT '最近一次消费时间，一般和订单的pay_time一致',
    predict_type int DEFAULT NULL COMMENT '预测类型: 0-E, 1-sme, 2-LR, 3-RNN, 4-ANN',
    predict_time datetime DEFAULT NULL COMMENT '预测消费时间',
    predict_dt double(20,6) DEFAULT NULL COMMENT '预测值(时间间隔，单位天)',
    p_accuracy double(20,6) DEFAULT NULL COMMENT '预估准确度',
    model_para text DEFAULT NULL COMMENT '模型参数',
    model_choice text DEFAULT NULL COMMENT '模型选择细节',
    obligate text DEFAULT NULL COMMENT '扩展',
    `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_time` timestamp DEFAULT '1990-01-01 00:00:00' ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY `IDX_MID` (MID),
KEY `IDX_MID` (MID),
KEY `IDX_pay_time` (`pay_time`) USING BTREE,
KEY `IDX_update_time` (`update_time`) USING BTREE
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='结果表-level2-消费预测表,一个粒度(一般是用户)只有一条预测结果)-';
'''

sql_cr_bm_r1_up_predict_record = '''
create table IF NOT EXISTS bm_r1_up_predict_record (
    `MID` binary(16) not null,
    pay_time datetime COMMENT '最近一次消费时间，一般和订单的pay_time一致',
    predict_type int DEFAULT NULL COMMENT '预测类型: 0-E, 1-sme, 2-LR, 3-RNN, 4-ANN',
    predict_time datetime DEFAULT NULL COMMENT '预测消费时间',
    predict_dt double(20,6) DEFAULT NULL COMMENT '预测值(时间间隔，单位天)',
    p_accuracy double(20,6) DEFAULT NULL COMMENT '预估准确度',
    real_time datetime DEFAULT NULL COMMENT '真实的：下次消费时间',
    real_dt double(20,6) DEFAULT NULL COMMENT '真实的: 间隔时间',
    r_err double(20,6) DEFAULT NULL COMMENT '绝对误差(注意不是误差的绝对值)',
    d_err int(6) DEFAULT NULL COMMENT '常用指标:天误差',
    err_rate1 double(20,6) DEFAULT NULL COMMENT '误差率1',
    err_rate2 double(20,6) DEFAULT NULL COMMENT '误差率2',
    model_para text DEFAULT NULL COMMENT '模型参数',
    model_choice text DEFAULT NULL COMMENT '模型选择细节',
    obligate text DEFAULT NULL COMMENT '扩展',
    `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_time` timestamp DEFAULT '1990-01-01 00:00:00' ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY `IDX_MID_pay` (MID, pay_time),
KEY `IDX_MID` (MID),
KEY `IDX_pay_time` (`pay_time`) USING BTREE,
KEY `IDX_update_time` (`update_time`) USING BTREE
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='结果表-level1-用户行为预测历史记录';
'''

sql_cr_bm_m1_up_predict_model_record_x = """
create table IF NOT EXISTS bm_m1_up_predict_model_record_%s (
    `MID` binary(16) not null,
    Pay_Time datetime COMMENT '最近一次消费时间，一般和订单的Pay_Time一致',
    predict_type int DEFAULT NULL COMMENT '预测类型: 0-E, -1-sma, >=1为nn模型',
    predict_time datetime DEFAULT NULL COMMENT '预测消费时间',
    predict_dt double(20,6) DEFAULT NULL COMMENT '预测值(时间间隔，单位天)',
    p_accuracy double(20,6) DEFAULT NULL COMMENT '预估准确度',
    real_time datetime DEFAULT NULL COMMENT '真实的：下次消费时间',
    real_dt double(20,6) DEFAULT NULL COMMENT '真实的: 间隔时间',
    r_err double(20,6) DEFAULT NULL COMMENT '绝对误差(注意不是误差的绝对值)',
    d_err int(6) DEFAULT NULL COMMENT '常用指标:天误差',
    err_rate1 double(20,6) DEFAULT NULL COMMENT '误差率1',
    err_rate2 double(20,6) DEFAULT NULL COMMENT '误差率2',
    model_para text DEFAULT NULL COMMENT '模型参数',
    model_choice text DEFAULT NULL COMMENT '模型选择细节',
    obligate text DEFAULT NULL COMMENT '扩展',
    `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_time` timestamp DEFAULT '1990-01-01 00:00:00' ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY `IDX_MID_pay` (MID, pay_time),
KEY `IDX_MID` (MID),
KEY `IDX_pay_time` (`pay_time`) USING BTREE,
KEY `IDX_update_time` (`update_time`) USING BTREE
)ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='中间表-level1-用户行为预测单模型表-model_%s';
"""

