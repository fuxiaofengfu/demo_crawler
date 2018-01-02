#-*- encoding: utf-8 -*-
'''
Created on 2017/7/1 13:49
Copyright (c) 2017/7/1, 海牛学院版权所有.
@author: 青牛
'''
import sys
sys.path.append('/home/qingniu/hainiu_crawler')

from commons.util.db_util import DBUtil
from commons.util.log_util import LogUtil
from configs import config
import time,random


def push_queue_items():
    count_news_seed_queue_sql = """select count(*) from hainiu_queue where type=3 and fail_times=0;"""
    insert_news_seed_internally_queue_items_sql = """
        insert into hainiu_queue (type,action,params) values(3,%s,%s);
    """
    count_news_seed_internally_sql = """select count(*) from hainiu_web_seed_internally where status=0 for update;"""
    selec_news_seed_internally_sql = """select a_url,param,id from hainiu_web_seed_internally where status=0 limit %s,%s;"""
    update_news_seed_internally_sql = """update hainiu_web_seed_internally set status=1 where id in (%s);"""
    rl = LogUtil().get_base_logger()
    try:
        d = DBUtil(config._HAINIU_DB)
        queue_total = d.read_one(count_news_seed_queue_sql)[0]
        if queue_total != 0:
            rl.info('last download_page queue not finish,last queue %s unFinish' % (queue_total))
            return


        starttime = time.clock()
        d = DBUtil(config._HAINIU_DB)
        total = long(d.read_one(count_news_seed_internally_sql)[0])
        page_size = 1000
        page = total / page_size
        for i in range(0, page + 1):
            sql = selec_news_seed_internally_sql % (0, page_size)
            list = d.read_tuple(sql)
            values = []
            id_values = []
            for l in list:
                url = l[0]
                url = url if url is not None else ''
                param = l[1]
                param = param if param is not None else ''
                values.append((url,param))
                id = l[2]
                id_values.append(str(id))
            if id_values.__len__() != 0:
                random.shuffle(values)
                d.executemany_no_commit(insert_news_seed_internally_queue_items_sql,values)
                ids = ','.join(id_values)
                sql = update_news_seed_internally_sql % (ids)
                d.execute(sql)
        endtime = time.clock()
        worksec = int(round((endtime - starttime)))
        rl.info('push seed_internally queue finish,total items %s,action time %s\'s' % (total,worksec))
    except:
        rl.exception()
        rl.error(sql)
        d.rollback()
    finally:
        d.close()

if __name__ == '__main__':
    push_queue_items()