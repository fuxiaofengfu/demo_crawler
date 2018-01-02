#-*- encoding: utf-8 -*-
'''
Created on 2017/7/1 13:49
Copyright (c) 2017/7/1, 海牛学院版权所有.
@author: 青牛
'''
import time, Queue, sys, MySQLdb, urllib2, json, mx.URL
sys.path.append('/home/qingniu/hainiu_crawler')

from commons.action.base_producer_action import ProducerAction
from commons.action.base_consumer_action import ConsumerAction
from commons.action.queue_producer import Producer
from commons.action.queue_consumer import Consumer
from commons.util.time_util import TimeUtil
from commons.util.log_util import LogUtil
from commons.util.db_util import DBUtil
from commons.util.util import Util
from commons.util.html_util import HtmlUtil
from commons.util.request_util import RequestUtil
from bs4 import BeautifulSoup
from datetime import datetime
from configs import config
from tld import get_tld

queue_name = 'hainiunewsfind'
class NewsFindProducer(ProducerAction):
    def __init__(self, limit, fail_times):
        self.limit = limit
        self.fail_times = fail_times
        self.rl = LogUtil().get_logger('producer', 'producer' + queue_name)


    def queue_items(self):
        select_queue_sql = """
            select id,action,params from hainiu_queue where type=1 and fail_times <= %s limit 0,%s for UPDATE;
        """
        update_queue_sql = """
            update hainiu_queue set type=0 where id in (%s);
        """
        list = []
        try:
            d = DBUtil(config._HAINIU_DB)
            sql = select_queue_sql % (self.fail_times,self.limit)
            tuple = d.read_tuple(sql)
            if len(tuple) == 0:
                return list
            queue_ids = ''
            for t in tuple:
                queue_id = t[0]
                url = t[1]
                param = '' if t[2] is None else t[2]
                queue_ids += str(queue_id) + ','
                c = NewsFindConsumer(url, param, queue_id)
                list.append(c)
            queue_ids = queue_ids[:-1]
            d.execute(update_queue_sql % (queue_ids))
        except:
            self.rl.exception()
            d.rollback()
            d.commit()
        finally:
            d.close()
        return list


class NewsFindConsumer(ConsumerAction):
    def __init__(self, url, param ,queue_id):
        ConsumerAction.__init__(self)
        self.url = url[:-1] if url.endswith('/') else url
        self.param = param
        self.queue_id = queue_id
        self.rl = LogUtil().get_logger('consumer', 'consumer' + queue_name)

    def action(self):
        is_success = True
        t = TimeUtil()
        u = Util()
        hu = HtmlUtil()
        r = RequestUtil()
        in_values = []
        ex_values = []
        a_href = ''
        main_md5 = u.get_md5(self.url)
        now_time = datetime.now()
        update_time = int(time.mktime(now_time.timetuple()))
        create_time = update_time
        create_day = int(t.now_day().replace('-', ''))
        create_hour = int(t.now_hour())
        try:
            html = r.http_get_phandomjs(self.url)
            domain = get_tld(self.url)

            soup = BeautifulSoup(html, 'lxml')
            a_docs = soup.find_all("a")
            a_set = set()
            a_param = {}
            out_json_srt = ''
            status = 0
            host = hu.get_url_host(self.url)

            for a in a_docs:
                a_href = self.get_format_url(a,host)
                a_title = a.get_text().strip()
                if a_href == '' or a_title == '':
                    continue
                if a_set.__contains__(a_href):
                    continue
                a_set.add(a_href)

                req = urllib2.Request(url=a_href)
                a_host = req.get_host() if req.get_host() is not None else ''
                a_md5 = u.get_md5(a_href)

                if a_title != '':
                    a_param['title'] = a_title
                    out_json_srt = json.dumps(a_param,ensure_ascii=False)

                a_xpath = hu.get_dom_parent_xpath_js(a)
                insert_values = (main_md5,domain,host,a_md5,a_host,a_xpath,create_time,create_day,create_hour,update_time,status,
                                 MySQLdb.escape_string(self.url),
                                 MySQLdb.escape_string(a_href),
                                 MySQLdb.escape_string(a_title),
                                 out_json_srt)

                if a_host.__contains__(domain):
                    in_values.append(insert_values)
                else:
                    ex_values.append(insert_values)

            in_table = 'hainiu_web_seed_internally'
            ex_table = 'hainiu_web_seed_externally'
            insert_sql = """
                insert into <table> (md5,domain,host,a_md5,a_host,a_xpath,create_time,create_day,create_hour,update_time,status,url,a_url,a_title,param)
                      values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE update_time=update_time;
            """
            try:
                d = DBUtil(config._HAINIU_DB)
                d.execute_no_commit("set NAMES utf8mb4;")
                if in_values.__len__() != 0:
                    sql = insert_sql.replace('<table>',in_table)
                    d.executemany_no_commit(sql,in_values)
                if ex_values.__len__() != 0:
                    sql = insert_sql.replace('<table>',ex_table)
                    d.executemany_no_commit(sql,ex_values)
                d.commit()
            except:
                is_success = False
                self.rl.exception()
                self.rl.error(sql)
                d.rollback()
            finally:
                d.close()

        except:
            is_success = False
            self.rl.exception()
        finally:
            r.close_phandomjs()

        return super(self.__class__, self).result(is_success, [main_md5,self.url,a_href,in_values.__len__(),ex_values.__len__(),self.queue_id])


    def success_action(self, values):
        delete_sql = """
            delete from hainiu_queue where id=%s;
        """
        update_hainiu_news_seed_sql = """
            update hainiu_web_seed set last_crawl_internally=%s,last_crawl_externally=%s,last_crawl_time=now() where md5="%s";"""
        try:
            d = DBUtil(config._HAINIU_DB)
            id = values[5]
            sql = delete_sql % id
            d.execute_no_commit(sql)
            sql = update_hainiu_news_seed_sql % (values[3],values[4],values[0])
            d.execute_no_commit(sql)
            d.commit()
        except:
            self.rl.exception()
            self.rl.error(sql)
            d.rollback()
            d.commit()
        finally:
            d.close()


    def fail_action(self, values):
        update_sql = """
            update hainiu_queue set fail_times=fail_times+1,fail_ip='%s' where id=%s;
        """
        update_sql_1 = """
            update hainiu_queue set type=1 where id=%s;
        """
        update_hainiu_news_seed_sql = """
            update hainiu_web_seed set fail_times=fail_times+1,fail_ip="%s" where md5="%s";
        """
        try:
            d = DBUtil(config._HAINIU_DB)
            id = values[5]
            u = Util()
            ip = u.get_local_ip()
            sql = update_sql % (ip, id)
            d.execute_no_commit(sql)
            main_md5 = values[0]
            sql = update_hainiu_news_seed_sql % (ip, main_md5)
            d.execute_no_commit(sql)
            if (self.try_num == Consumer._WORK_TRY_NUM):
                sql = update_sql_1 % (id)
                d.execute_no_commit(sql)
            d.commit()
        except:
            self.rl.exception()
            self.rl.error(sql)
            d.rollback()
            d.commit()
        finally:
            d.close()


    def get_format_url(self, a_doc, host):
        a_href = a_doc.get('href')
        try:
            if a_href is not None and a_href.__len__() > 0:
                a_href = str(a_href).strip()
                a_href = a_href[:a_href.index('#')] if a_href.__contains__('#') else a_href
                # a_href = a_href.encode('utf8')
                # a_href = urllib.quote(a_href,safe='.:/?&=')
                if a_href.startswith('//'):
                    url = 'https:' + a_href if self.url.startswith('https:') else 'http:' + a_href
                    url = mx.URL.URL(str(url))
                    a_href = url.url
                elif a_href.startswith('/'):
                    url = 'https://' + host + a_href if self.url.startswith('https:') else 'http://' + host + a_href
                    url = mx.URL.URL(str(url))
                    a_href = url.url
                elif a_href.startswith('./') or a_href.startswith('../'):
                    url = mx.URL.URL(str(self.url) + '/' + a_href)
                    a_href = url.url
                elif not a_href.startswith('javascript') and not a_href.startswith('mailto') and not a_href.startswith('http') and a_href != '':
                    url = 'https://' + host + '/' + a_href if self.url.startswith('https:') else 'http://' + host + '/' + a_href
                    url = mx.URL.URL(str(url))
                    a_href = url.url
                a_href = a_href[:-1] if a_href.endswith('/') else a_href
                # a_href = a_href.lower()
            get_tld(a_href)
        except:
            return ''

        if not a_href.startswith('http'):
            return ''

        if a_href.__contains__('?'):
            a_params_str = a_href[a_href.index('?') + 1:]
            a_params = a_params_str.split('&')
            a_params.sort()
            a_params_str = '&'.join(a_params)
            a_href = a_href[:a_href.index('?') + 1] + a_params_str

        return a_href


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')
    q = Queue.Queue()
    config_ = config._FIND_NEWS_CONFIG
    pp = NewsFindProducer(config_['QUERY_LIMIT'],config_['QUERY_FAIL_TIMES'])
    p = Producer(q, pp, queue_name, config_['WORK_THREAD_NUMS'], config_['QUEUE_SLEEP_TIMES'], config_['WORK_SLEEP_TIMES'], config_['FAIL_RETRY_TIMES'])
    p.start_work()