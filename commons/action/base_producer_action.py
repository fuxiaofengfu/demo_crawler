#-*- encoding: utf-8 -*-
'''
base_producer_action.py
Created on 2017/12/22 11:14
Copyright (c) 2017/12/22, 海牛学院版权所有.
@author: 青牛
'''
class ProducerAction(object):
    '''
    生产者的基类
    '''


    def queue_items(self):
        '''
        得到消费任务，用于放入到队列中，供消费者进程使用

        :return:       消费动作
        '''
        pass