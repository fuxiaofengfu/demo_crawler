#-*- encoding: utf-8 -*-
'''
Created on 2017/7/1 13:49
Copyright (c) 2017/7/1, 海牛学院版权所有.
@author: 青牛
'''
import time, Queue, sys, os, shutil, MySQLdb
sys.path.append('/home/qingniu/hainiu_crawler')

from commons.action.base_producer_action import ProducerAction
from commons.action.base_consumer_action import ConsumerAction
from commons.action.queue_producer import Producer
from commons.action.queue_consumer import Consumer
from commons.util.time_util import TimeUtil
from commons.util.file_util import FileUtil
from commons.util.log_util import LogUtil
from commons.util.db_util import DBUtil
from commons.util.util import Util
from commons.util.html_util import HtmlUtil
from commons.util.request_util import RequestUtil
# from download_page.util.kafka_util import KafkaUtil
from bs4 import BeautifulSoup
from datetime import datetime
from configs import config
from util import content
from tld import get_tld

queue_name = 'hainiunewsdownloader'
class DownLoadProducer(ProducerAction):
    def __init__(self, limit, pro_flag, fail_times):
        self.limit = limit
        self.fail_times = fail_times
        self.pro_flag = pro_flag
        self.rl = LogUtil().get_logger('producer', 'producer' + queue_name)

    def queue_items(self):
        select_queue_sql = """
            select id,action,params from hainiu_queue where type=3 and fail_times <= %s limit 0,%s for UPDATE;
        """
        update_queue_sql = """
            update hainiu_queue set type=2 where id in (%s);
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
                c = DownLoadConsumer(url, param, queue_id, self.pro_flag)
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

html_file_path_cache = {}
class DownLoadConsumer(ConsumerAction):
    def __init__(self, url, param, queue_id, pro_flag):
        ConsumerAction.__init__(self)
        self.url = url[:-1] if url.endswith('/') else url
        self.param = param
        self.queue_id = queue_id
        self.pro_flag = pro_flag
        self.rl = LogUtil().get_logger('consumer', 'consumer' + queue_name)

    def action(self):
        is_success = True
        t = TimeUtil()
        f = FileUtil()
        u = Util()
        hu = HtmlUtil()
        r = RequestUtil()
        values = []
        md5 = u.get_md5(self.url)
        now_time = datetime.now()
        update_time = int(time.mktime(now_time.timetuple()))
        create_time = update_time
        create_day = int(t.now_day().replace('-', ''))
        create_hour = int(t.now_hour())
        now_minute = int(t.now_min())
        for i in xrange(60,-5,-5):
            if now_minute>=i:
                now_minute=i
                break
        now_minute = t.now_time(format='%Y%m%d%H') + ('0%s' % (str(now_minute)) if now_minute < 10 else str(now_minute))

        values.append(MySQLdb.escape_string(self.url))
        values.append(md5)
        values.append(create_time)
        values.append(create_day)
        values.append(create_hour)
        values.append('')
        values.append(MySQLdb.escape_string(self.param))
        values.append(update_time)
        try:
            html = r.http_get_phandomjs(self.url)
            domain = get_tld(self.url)
            values[5] = domain

            soup = BeautifulSoup(html, 'lxml')
            title_doc = soup.find('title')
            title = title_doc.contents[0] if title_doc is not None and len(title_doc.contents) == 1 else ''

            host = hu.get_url_host(self.url)
            values.append(host)
            values.append(MySQLdb.escape_string(title))

            # k = KafkaUtil(config._KAFKA_CONFIG)
            html = html.replace(content._SEQ1,'').replace(content._SEQ2,content._SEQ4)
            # push_str = content._SEQ3.join(('%s','%s')) % (self.url,html)
            # push_str = content._SEQ3.join(('%s','%s')) % (u.get_md5(push_str),push_str)
            # push_str = bytes(push_str)
            # is_success = k.push_message(push_str)

            is_success = True
            if is_success:
                self.save_file(create_time,f,now_minute,u,self.url,html)
            else:
                values.append('')
                values.append('')
                self.rl.error("kafka push error")

        except:
            is_success = False
            values.append('')
            values.append('')
            self.rl.exception()
        finally:
            r.close_phandomjs()

        try:
            if is_success:
                values.append(1)
                insert_web_page_sql = """
                    insert into hainiu_web_page (url,md5,create_time,create_day,create_hour,domain,param,update_time,host,
                    title,status) values ("%s","%s",%s,%s,%s,"%s","%s",%s,"%s","%s",%s) on DUPLICATE KEY  UPDATE update_time=values(update_time);
                """
            else:
                ip = u.get_local_ip()
                values.append(ip)
                values.append(2)
                insert_web_page_sql = """
                    insert into hainiu_web_page (url,md5,create_time,create_day,create_hour,domain,param,update_time,host,
                    title,fail_ip,status) values ("%s","%s",%s,%s,%s,"%s","%s",%s,"%s","%s","%s",%s)
                    on DUPLICATE KEY UPDATE fail_times=fail_times+1,fail_ip=values(fail_ip);
                """

            d = DBUtil(config._HAINIU_DB)
            sql = insert_web_page_sql % tuple(values)
            d.execute(sql)
        except:
            is_success = False
            self.rl.exception()
            self.rl.error(sql)
            d.rollback()
            d.commit()
        finally:
            d.close()


        return super(self.__class__, self).result(is_success, [md5,self.url,update_time,self.queue_id])


    def success_action(self, values):
        delete_sql = """
            delete from hainiu_queue where id=%s;
        """
        update_hainiu_news_internally_sql = """
            update hainiu_web_seed_internally set update_time=%s where md5="%s";
        """
        try:
            d = DBUtil(config._HAINIU_DB)
            id = values[3]
            sql = delete_sql % id
            d.execute_no_commit(sql)
            sql = update_hainiu_news_internally_sql % (values[2],values[0])
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
            update hainiu_queue set type=3 where id=%s;
        """
        update_hainiu_news_internally_sql = """
            update hainiu_web_seed_internally set fail_times=fail_times+1,fail_ip="%s",update_time=%s where md5="%s";
        """
        try:
            d = DBUtil(config._HAINIU_DB)
            id = values[3]
            u = Util()
            ip = u.get_local_ip()
            sql = update_sql % (ip, id)
            d.execute_no_commit(sql)
            sql = update_hainiu_news_internally_sql % (ip, values[2], values[0])
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


    def save_file(self, create_time, fi, now_minute, u, url, html):
        now_file_name = '%s_%s_%s' % (self.consumer_thread_name, self.pro_flag, now_minute)
        last_file_name = u.get_dict_value(html_file_path_cache, self.consumer_thread_name)
        html_file_path_cache[self.consumer_thread_name] = now_file_name
        tmp_path = config._LOCAL_DATA_DIR % ('%s/%s_%s' % ('tmp', self.consumer_thread_name, self.pro_flag))
        start_char = content._SEQ2
        if last_file_name is None or now_file_name != last_file_name:
            start_char = ''
            if os.path.exists(tmp_path) and os.path.getsize(tmp_path) > 0:
                done_path = config._LOCAL_DATA_DIR % ('%s/%s_%s' % ('done', now_file_name, create_time))
                shutil.move(tmp_path, done_path)

        html = html.replace(content._SEQ1,'').replace(content._SEQ2,content._SEQ4)
        record_str = content._SEQ3.join(('%s','%s')) % (url,html)
        record_str = content._SEQ3.join(('%s','%s')) % (u.get_md5(record_str),record_str)
        html_record_format_str = start_char + record_str
        fi.write_file_content_pattern(tmp_path, html_record_format_str, pattern='a')


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')
    q = Queue.Queue()
    config_ = config._DOWN_CONFIG
    pp = DownLoadProducer(config_['QUERY_LIMIT'],config_['FILE_FLAG'],config_['QUERY_FAIL_TIMES'])
    p = Producer(q, pp, queue_name, config_['WORK_THREAD_NUMS'], config_['QUEUE_SLEEP_TIMES'], config_['WORK_SLEEP_TIMES'], config_['FAIL_RETRY_TIMES'])
    p.start_work()
