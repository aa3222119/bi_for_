# ===========================================
# @Time    : 2019/4/10 17:29
# @Author  : antony
# @Email   : 502202879@qq.com
# @File    : components.py
# @Software: PyCharm
# ===========================================


class WebMethod:
    ver_funcs = {}

    def __init__(self, version, functions):
        self.info = 'a class for restful api. which has a simple methods versions control'
        self.ver = version.replace('.', '_')
        if functions:
            self.reg_ver_funcs(functions)

    def reg_ver_funcs(self, functions):
        # 注册版本控制的functions
        this_ver_funcs = {func.__name__: func for func in functions}
        self.ver_funcs.update({self.ver: this_ver_funcs})
        return self

    def setattr_ver_funcs(self, functions):
        # 同样的 也可以利用反射原理来做版本管理
        for func in functions:
            self.__setattr__(func.__name__ + self.ver, func)


class WebMethodVerCon:
    funcs_versions = {}

    def __init__(self, version, functions):
        self.info = 'a class for restful api. which has a simple methods versions control for bark2'
        for func in functions:
            self.reg_ver_funcs(version, func)

    def reg_ver_funcs(self, version, func):
        # 注册版本控制的functions
        this_func_ver = {version: func}
        if func.__name__ in self.funcs_versions:
            self.funcs_versions[func.__name__].update(this_func_ver)
            # 纯字符串的版本号大小管理 默认取最大的版本号
            if version > self.funcs_versions[func.__name__]['latest_ver']:
                self.funcs_versions[func.__name__]['latest_ver'] = version
        # 这是这个func的第一个版本
        else:
            self.funcs_versions[func.__name__] = this_func_ver
            self.funcs_versions[func.__name__]['latest_ver'] = version
        # 重置一下函数本身的名称
        func.__name__ = func.__name__ + '_v' + version
        return self
