# handle_loguru
from configparser import ConfigParser  # 读取ini配置的模块
from loguru import logger  # 日志模块
from heandle_path import config_path  # 读取项目自定义路径
from time import strftime, sleep  # 时间模块


class MyLog:
    __instance = None  # 单例实现
    __call_flag = True  # 控制init调用，如果调用过就不再调用

    def __new__(cls, *args, **kwargs):
        if not cls.__instance:
            cls.__instance = super().__new__(cls)
        return cls.__instance

    def get_log(self):
        if self.__call_flag:  # 看是否调用过
            __curdate = strftime('%Y%m%d-%H%M%S')
            cfg = ConfigParser()  # 读取ini文件的一个实例对象
            cfg.read(f'{config_path}\loguru.ini')  # 读取配置文件
            logger.remove(handler_id=None)  # 关闭console输出
            logger.add(f'log_{__curdate}.log',  # 日志存放位置
                       retention=cfg.get('log', 'retention'),  # 清理
                       rotation=cfg.get('log', 'rotation'),  # 循环 达到指定大小后建立新的日志
                       format=cfg.get('log', 'format'),  # 日志输出格式
                       compression=cfg.get('log', 'compression'),  # 日志压缩格式
                       level=cfg.get('log', 'level'),  # 日志级别
                       encoding='utf-8')
            self.__call_flag = False  # 如果调用过就置为False
        return logger


if __name__ == '__main__':
    log1 = MyLog().get_log()  # 第一个实例
    log1.error('李四')
    sleep(5)
    log2 = MyLog().get_log()  # 第二个实例,因为做了单例模式,所以其实是同一个实例
    log2.error('张三')
