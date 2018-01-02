#-*- encoding: utf-8 -*-
'''
hainiu_queue.py
Created on 2017/12/23 12:15
Copyright (c) 2017/12/23, 海牛学院版权所有.
@author: 青牛
'''

from commons.util.log_util import LogUtil
from commons.util.db_util import DBUtil
from configs import config
import sys

def push_queue_items():
    inert_sql = """
    insert into hainiu_queue (type,params,action) values(1,%s,%s);
    """
    count_sql = """
    select count(1) from hainiu_queue where type=1;
    """
    select_sql = """
    select id from hainiu_queue where type=1 limit %s,%s;
    """
    rl = LogUtil().get_base_logger()
    try:
        d = DBUtil(config._HAINIU_DB)
        sql = inert_sql
        insert_list = [("aaa", "bbb"), ("dffddf", "awwee")]
        d.executemany(sql, insert_list)


        sql = count_sql
        queue_total = d.read_one(sql)[0]
        print "queue_total",queue_total
        page_size = 10
        page = (queue_total/page_size) + 1
        print "page",page


        for i in range(0,page):
            sql = select_sql % (i * page_size,page_size)
            select_list = d.read_tuple(sql)
            print "page",i
            for record in select_list:
                id = record[0]
                print id



    except:
        rl.exception()
        rl.error(sql)
        d.rollback()
    finally:
        d.close()

if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf-8')
    push_queue_items()