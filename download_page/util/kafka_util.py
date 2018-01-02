#-*- encoding: utf-8 -*-
'''
Created on 2015/11/17
Copyright (c) 2015, 金山网络版权所有.
@author: 何青
'''

from pykafka import KafkaClient
from commons.util.util import Util
from commons.util.log_util import LogUtil
import random,threading

class KafkaUtil:

    __kafka_connect_cache = {}

    __lock = threading.Lock()

    def __init__(self,kafka_conf):
        host_list = [host for host in kafka_conf['HOST'].split(',')]
        random.shuffle(host_list)
        host_str = ','.join(host_list)
        self.cache_key = '_'.join((host_str,kafka_conf['TOPIC']))
        self.host = host_str
        self.topic = kafka_conf['TOPIC']
        self.rl = LogUtil().get_logger('consumer', 'consumer_kafka')


    def push_message(self,message):
        self.__lock.acquire()
        u = Util()
        producer = u.get_dict_value(self.__kafka_connect_cache,self.cache_key)
        if producer is None:
            client = KafkaClient(hosts=self.host)
            topic = client.topics[self.topic]
            producer = topic.get_producer()
            self.__kafka_connect_cache[self.cache_key] = producer

        is_success = True
        try:
            producer.produce(message)
        except:
            is_success = False
            del self.__kafka_connect_cache[self.cache_key]
            self.rl.error('kafka push error cacheKey is %s' % (self.cache_key))
            self.rl.exception()

        self.__lock.release()
        return is_success

