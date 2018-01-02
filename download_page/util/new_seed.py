#-*- encoding: utf-8 -*-
'''
new_seed.py.py
Created on 2017/10/19 11:21
Copyright (c) 2017/10/19, 海牛学院版权所有.
@author: 青牛
'''

from commons.util.log_util import LogUtil
from commons.util.db_util import DBUtil
from commons.util.html_util import HtmlUtil
from configs import config
from tld import get_tld
from commons.util.util import Util
import sys

def create_seed():
    url = "http://mil.huanqiu.com/china"
    catetory = "军事"
    sql = """
    insert into hainiu_web_seed (url,md5,domain,host,category,status) values
    ('%s','%s','%s','%s','%s',0);
    """
    hu = HtmlUtil()
    domain = get_tld(url)
    host = hu.get_url_host(url)
    u = Util()
    md5 = u.get_md5(url)

    rl = LogUtil().get_base_logger()
    try:
        d = DBUtil(config._HAINIU_DB)
        sql = sql % (url,md5,domain,host,catetory)
        d.execute(sql)
    except:
        rl.exception()
        d.rollback()
    finally:
        d.close()


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')
    create_seed()