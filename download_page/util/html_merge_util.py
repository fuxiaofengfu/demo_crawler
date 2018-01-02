#-*- encoding: utf-8 -*-
'''
Created on 2017/7/1 13:49
Copyright (c) 2017/7/1, 海牛学院版权所有.
@author: 青牛
'''

import os, sys, time, shutil, traceback
sys.path.append('/home/qingniu/hainiu_crawler')

from commons.util.file_util import FileUtil
from commons.util.time_util import TimeUtil
from send_sms_util import SendSmsUtil
from commons.util.util import Util
from datetime import datetime
from configs import config


def date_merge():
    u = Util()
    fi = FileUtil()
    t = TimeUtil()
    s = SendSmsUtil()
    alter_time = t.now_time()
    beijing_now = datetime.now()
    now_time = int(time.mktime(beijing_now.timetuple()))
    tmp_path = config._LOCAL_DATA_DIR % ('%s/%s_%s.tmp' % ('tmp','hainiu', now_time))
    up_path = config._LOCAL_DATA_DIR % ('%s/%s_%s.done' % ('up','hainiu', now_time))
    start_char = ''
    for dirpath, dirnames, filenames in os.walk(config._LOCAL_DATA_DIR % ('done')):
        for filename in filenames:
            total = 0
            merge_total = 0
            dir = os.path.join(dirpath, filename)
            file_size = os.path.getsize(dir)
            record_list = []
            with open(dir) as f:
                for line in f:
                    try:
                        total += 1
                        line = line.strip().encode('utf-8')
                        if not line:
                            continue
                        md5 = line[:line.find('\001')]
                        record = line[line.find('\001') + 1:]
                        record_md5 = u.get_md5(record)
                        if md5 == record_md5:
                            merge_total += 1
                            record_list.append(record)
                        else:
                            raise Exception('check is faild')

                        if record_list.__len__() >=10:
                            fi.write_file_content_pattern(tmp_path,start_char + ('\n'.join(record_list)), pattern='a')
                            record_list = []
                            start_char = '\n'
                    except Exception:
                        traceback.print_exc()
                        print line
                        alter_msg = 'alter merge api hainiu time:%s ip:%s' % (alter_time, u.get_local_ip())
                        s.send_sms(alter_msg)

            if record_list.__len__() >0:
                fi.write_file_content_pattern(tmp_path,start_char + ('\n'.join(record_list)), pattern='a')
                start_char = '\n'

            os.remove(dir)
            print dir,file_size,total,merge_total

    if os.path.exists(tmp_path) and os.path.getsize(tmp_path) > 0:
        shutil.move(tmp_path, up_path)


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')
    date_merge()