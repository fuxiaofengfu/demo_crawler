# -*- encoding: utf-8 -*-
'''
hainiu_action.py
Created on 2017/12/23 11:03
Copyright (c) 2017/12/23, 海牛学院版权所有.
@author: 青牛
'''

from commons.action.base_consumer_action import ConsumerAction
from commons.action.base_producer_action import ProducerAction
from commons.util.log_util import LogUtil
from commons.util.db_util import DBUtil
from commons.util.util import Util
from commons.action.queue_producer import Producer
from commons.action.queue_consumer import Consumer
from configs import config
import Queue, sys

queue_name = "hainiuqueue"


class HainiuProducer(ProducerAction):
    def __init__(self, limit, fail_times):
        '''
        初始化队列的发者

        :param limit:           每次从队列中取多少条记录
        :param fail_times:      限定取记录的失败次数条件
        '''
        super(self.__class__, self).__init__()
        self.limit = limit
        self.fail_times = fail_times
        self.rl = LogUtil().get_logger('producer', 'producer' + queue_name)

    def queue_items(self):
        '''
        从队列中取出要处理的消息，并封装成消费者动作，然后更新队列的状态
        :return:            封装好的消费者动作列表
        '''

        # 会限制本机处理失败之后就不再进行获取的获取，通过机器IP来限制
        # select_queue_sql = """
        # select id,action,params from hainiu_queue where type=1 and fail_ip <>'%s' and fail_times<=%s
        # limit 0,%s for update;
        # """

        select_queue_sql = """
        select id,action,params from hainiu_queue where type=1 and fail_times<=%s
        limit 0,%s for update;
        """

        update_queue_sql = """
        update hainiu_queue set type=0 where id in (%s);
        """
        return_list = []
        try:
            d = DBUtil(config._HAINIU_DB)
            # u = Util()
            # ip = u.get_local_ip()
            # sql = select_queue_sql % (self.fail_times,ip,self.limit)
            sql = select_queue_sql % (self.fail_times, self.limit)
            select_dict = d.read_dict(sql)
            if len(select_dict) == 0:
                return return_list

            query_ids = []
            for record in select_dict:
                id = record["id"]
                action = record["action"]
                params = record["params"]
                query_ids.append(str(id))
                c = HainiuConsumer(id, action, params)
                return_list.append(c)

            ids = ",".join(query_ids)
            sql = update_queue_sql % ids
            d.execute(sql)
        except:
            self.rl.exception()
            self.rl.error(sql)
            d.rollback()
        finally:
            d.close()
        return return_list


class HainiuConsumer(ConsumerAction):
    def __init__(self, id, ac, params):
        '''
        初始化队列的消费者

        :param id:          消息的ID，也就是数据库表里的ID
        :param ac:          消息的动作信息，也就是数据库表里的action字段
        :param params:      消息的动作的附加参数
        '''
        super(self.__class__, self).__init__()
        self.id = id
        self.ac = ac
        self.params = params
        self.rl = LogUtil().get_logger('consumer', 'consumer' + queue_name)

    def action(self):
        '''
        处理拿到的消息

        :return:消费动作的处理结果，用于消费者线程的日志打印和传递处理成功和失败方法所需要的数据
        '''
        is_success = True
        try:
            print self.ac, self.params
            # 1/0
        except:
            is_success = False
            self.rl.exception()

        return super(self.__class__, self).result(is_success, [self.id])

    def success_action(self, values):
        '''
        消息动作处理成功之后，从队列中间件删除该消息，表示这个消息最终处理完成

        :param values:      消息动作处理之后的结果
        '''
        delete_sql = """
           delete from hainiu_queue where id=%s
        """
        try:
            d = DBUtil(config._HAINIU_DB)
            id = values[0]
            sql = delete_sql % id
            d.execute(sql)
        except:
            self.rl.exception()
            self.rl.error(sql)
            d.rollback()
        finally:
            d.close()

    def fail_action(self, values):
        '''
        消息动作处理失败之后，更改队列中间件中该消息的失败次数并记录执行机器的IP
        如果达到该机器的最大尝试失败次数，则更改队列中间件中该消息的状态为未处理，目的让其它机器再次尝试去处理该消息

        :param values:      消息动作处理之后的结果
        '''
        update_sql = """
            update hainiu_queue set fail_times=fail_times+1,fail_ip='%s' where id=%s;
        """
        update_sql_1 = """
            update hainiu_queue set type=1 where id=%s
        """
        try:
            d = DBUtil(config._HAINIU_DB)
            id = values[0]
            u = Util()
            ip = u.get_local_ip()
            sql = update_sql % (ip, id)
            d.execute_no_commit(sql)
            if (self.try_num == Consumer._WORK_TRY_NUM):
                sql = update_sql_1 % id
                d.execute_no_commit(sql)
            d.commit()
        except:
            self.rl.exception()
            self.rl.error(sql)
            d.rollback()
        finally:
            d.close()


if __name__ == "__main__":
    # 设置程序的字符集
    reload(sys)
    sys.setdefaultencoding('utf-8')

    q = Queue.Queue()
    pp = HainiuProducer(20, 6)
    p = Producer(q, pp, queue_name, 10, 2, 2, 3)
    p.start_work()
