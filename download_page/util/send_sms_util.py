#-*- encoding: utf-8 -*-
'''
Created on 2017/7/1 13:49
Copyright (c) 2017/7/1, 海牛学院版权所有.
@author: 青牛
'''
from configs import config
from commons.util.log_util import LogUtil
import urllib2
import urllib

class SendSmsUtil:

    def send_sms(self, content, phone=config._ALERT_PHONE):
        """send alter sms for phone with content
        """
        l = LogUtil().get_base_logger()
        try:
            send_url = 'http://send.sms.hainiu.com:8080/s?command=cralwer&phone=%s&' % (phone)
            send_url += urllib.urlencode({'content':content.decode('utf-8').encode('gbk')})
            r = urllib2.urlopen(send_url).read()
            if '0-OK' != r:
                l.error("短信发送失败,短信服务器返回状态为:%s,手机号:%s,内容:%s" % (r, phone, content))
                return False
        except:
            l.exception()
            return False
        return True
        
    
if __name__ == '__main__':
    SendSmsUtil().send_sms('你好''110')
