#-*- encoding: utf-8 -*-
'''
demo_queue.py
Created on 2017/12/22 14:23
Copyright (c) 2017/12/22, 海牛学院版权所有.
@author: 青牛
'''
from commons.util.log_util import LogUtil
import base_producer_action,base_consumer_action,Queue,queue_producer,queue_consumer

class HainiuConsumerAction(base_consumer_action.ConsumerAction):

    def __init__(self,text):
        '''
        初始化消费者实现类

        :param text:        消费者要处理的数据
        :return:
        '''
        super(self.__class__,self).__init__()
        self.text = text
        self.rl = LogUtil().get_base_logger()


    def action(self):
        '''
        消费者的具体需求实现

        :return:    消费动作的处理结果，用于消费者线程的日志打印
        '''
        result = True
        r_test = ''
        try:
            r_test = "hainiu" + str(self.text)
            # 1/0
        except:
            # self.rl.exception()
            result = False

        return self.result(result,[r_test])


    def fail_action(self,values):
        if self.try_num >= queue_consumer.Consumer._WORK_TRY_NUM:
            pass

    def success_action(self,values):
        pass

class HainiuProducerAction(base_producer_action.ProducerAction):

    def queue_items(self):
        '''
        生成指定的消费者动作

        :return:    消费者动作的集合
        '''
        _list = []
        for i in range(0,200):
            c = HainiuConsumerAction(i)
            _list.append(c)
        return _list


if __name__ == "__main__":
    #初始化使用的队列
    q = Queue.Queue()
    #初始化生产者动作
    pp = HainiuProducerAction()
    #初始化生产者
    p = queue_producer.Producer(q,pp,"hainiu",100,10,3,3)
    #启动整个生产和消费任务
    p.start_work()
