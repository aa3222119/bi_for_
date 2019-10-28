import multiprocessing
import time


def always_dec(dt=60):
    """通用轮询
    """
    def wrapper(func):
        def _wrapper(*args, **k_args):
            while 1:
                func(*args, **k_args)
                time.sleep(dt)
        return _wrapper
    return wrapper


def polling_dec(dt=60):
    """通用轮询
    """
    def wrapper(func):
        def _wrapper(*args, ):
            while 1:
                p = multiprocessing.Process(target=func, args=(*args, ), )
                p.start()
                time.sleep(dt)
                p.join()
        return _wrapper
    return wrapper


def general_bgp(func, args=(), **k_args):
    """
    通用版后端进程bgp background process, 只有一个进程去做这个任务，间隔时间是任务结束到下次开始的间隔
    :param func:
    :param args:
    :param k_args:
    :return:
    """
    if 'dt' not in k_args:
        k_args['dt'] = 300

    @always_dec(k_args['dt'])
    def continue_do_sth(*args_, **k_args_):
        # print(args_, k_args_)
        func(*args_, **k_args_)

    p = multiprocessing.Process(target=continue_do_sth, args=args, )
    p.start()
    return p


def general_bgp2(func, args=(), **k_args):
    """
    通用版后端进程bgp background process, 每次用新的进程来执行，间隔时间是任务开始到开始的间隔
    :param func: 
    :param args: 
    :param k_args: 
    :return: 
    """
    if 'dt' not in k_args:
        k_args['dt'] = 300
    func_ = polling_dec(dt=k_args['dt'])(func)
    print(args)
    return multiprocessing.Process(target=func_, args=args, ).start()


def test_doing(glo, words):
    glo['test'] = words
    print(glo)


def test_b1(variable):
    general_bgp(test_doing, (variable, '测试一下polling_de装饰器下的continue_do是否成功'), dt=5)
