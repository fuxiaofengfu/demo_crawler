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
from tld import get_tld
import time,random,json,sys


def push_queue_items():
    count_news_seed_sql = """select count(*) from hainiu_web_seed where status=0;"""
    select_news_seed_sql = """select url,category,last_crawl_time from hainiu_web_seed where status=0 limit %s,%s;"""
    insert_news_seed_queue_items_sql = """insert into hainiu_queue (type,action,params) values(1,%s,%s);"""
    count_news_seed_queue_sql = """select count(*) from hainiu_queue where type=1 and fail_times=0;"""
    rl = LogUtil().get_base_logger()
    try:
        d = DBUtil(config._HAINIU_DB)
        queue_total = d.read_one(count_news_seed_queue_sql)[0]
        if queue_total != 0:
            rl.info('last news_find queue not finish,last queue %s unFinish' % (queue_total))
            return

        starttime = time.clock()
        total = long(d.read_one(count_news_seed_sql)[0])
        page_size = 1000
        page = total / page_size
        for i in range(0, page + 1):
            sql = select_news_seed_sql % (i * page_size, page_size)
            list = d.read_tuple(sql)
            values = []
            for l in list:
                url = l[0]
                publisher = get_tld(url)
                publisher = publisher[0:publisher.index(('.'))] if publisher.__contains__('.') else publisher
                param = {}
                param['category'] = l[1]
                param['publisher'] = publisher
                param = json.dumps(param,ensure_ascii=False)
                values.append((url,param))

            if values.__len__() != 0:
                random.shuffle(values)
                d.executemany(insert_news_seed_queue_items_sql,values)
        endtime = time.clock()
        worksec = int(round((endtime - starttime)))
        rl.info('push news_find queue finish,total items %s,action time %s\'s' % (total,worksec))
    except:
        rl.exception()
        rl.error(sql)
        d.rollback()
    finally:
        d.close()

if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')
    push_queue_items()