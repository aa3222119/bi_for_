import scipy as sp
import numpy as np
# 不用科学计数法显示
np.set_printoptions(suppress=True)
np.set_printoptions(precision=3)
import random

from sklearn.linear_model import LinearRegression
# sklearn.linear_model.LinearRegression   http://www.ppvke.com/Blog/archives/19208
from sklearn.cluster import KMeans


# LinearRegression http://blog.csdn.net/viewcode/article/details/8794401

def sigmoid(x):
    #  Implement sigmoid function
    return 1/(1 + np.exp(-x))


def SSE(y_test, y):
    return (sp.square(y_test - y)).sum()


def RMSE(y_test, y):
    """
    均方误差根
    :param y_test: 预测值
    :param y: 实际
    :return:
    """
    return sp.sqrt(sp.mean(sp.square(y_test - y)))


def SSR(y_test, y):
    return (sp.square(y_test - y.mean())).sum()


def SST(y):
    return (sp.square(y - y.mean())).sum()


def R2(y_test, y):
    """
    coefficient of determination 决定系数 #http://blog.sciencenet.cn/blog-651374-975670.html
    :param y_test:
    :param y:
    :return:
    """
    return 1 - SSE(y_test, y) / SST(y)


def nth_ladder_create(mat, n=3, col=-1):
    """
    构造阶梯属性(rnn 简化成 ann时使用) 实现RNN n阶数
    :param mat: 因素矩阵  样本数*因素维
    :param n: 阶梯的阶数
    :param col: 被选做构成阶梯的列(属性)
    :return:
    """
    mat_ = mat[n:, :]
    for i in range(1, n+1):
        mat_ = np.hstack((mat[n-i:-i, [col]], mat_))
    return mat_


# def kmeans_dprocess(x_var, n_clusters=4):
#     if x_var.__len__() < n_clusters:
#         return 'simple size small than n_clusters(%s)' % n_clusters
#     kmeans = KMeans(n_clusters=n_clusters)
#     r_kmeans = kmeans.fit(x_var)
#     x_mean = x_var.mean()
#     ind = r_kmeans.labels_ == kmeans.n_clusters
#     for mi in range(kmeans.n_clusters):
#         indt = r_kmeans.labels_ == mi
#         if r_kmeans.cluster_centers_[mi] > 2 * x_mean and r_kmeans.labels_[indt].__len__() / r_kmeans.labels_.__len__() < 0.144:
#             # print(uid,r_kmeans.cluster_centers_[mi],x_mean,r_kmeans.labels_[indt].__len__(),r_kmeans.labels_.__len__())
#             ind = ind | indt
#         if r_kmeans.cluster_centers_[mi] > max_dt:
#             ind = ind | indt
#     return ind, kmeans
