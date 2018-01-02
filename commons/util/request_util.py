#-*- encoding: utf-8 -*-
'''
Created on 2017/7/1 13:49
Copyright (c) 2017/7/1, 海牛学院版权所有.
@author: 青牛
'''
import urllib2, urllib,cookielib,threading,gzip,sys
from commons.util.file_util import FileUtil
from StringIO import StringIO
from selenium.webdriver import PhantomJS
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

class RequestUtil:
    __browserAgent = 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:39.0) Gecko/20100101 Firefox/39.0'

    def __init__(self):
        self.cookies=''
        self._lock = threading.RLock()

    def http_get_request(self, url, referer, timeout=''):
        self._lock.acquire()
        cookie = cookielib.CookieJar()
        opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cookie),SmartRedirectHandler())
        urllib2.install_opener(opener)
        headers = {'User-Agent': self.__browserAgent,
                     'Referer': referer,
                     'Cache-Control': 'max-age=0',
                     'Accept': '*/*',
                     'Connection': 'Keep-Alive',
                     'Accept-encoding':'gzip'}
        req = urllib2.Request(url=url,headers=headers)
        if timeout == '':
            open = urllib2.urlopen(req)
        else:
            open = urllib2.urlopen(req, timeout=timeout)
        if self.cookies == '':
            for item in cookie:
                self.cookies = self.cookies + item.name + '=' + item.value + ';'
            self.cookies = self.cookies[:-1]
        if url != open.url:
            req = urllib2.Request(url=open.url,headers=headers)
        self._lock.release()
        return (open,req)

    def http_post_request(self, url, datas, referer, timeout=''):
        self._lock.acquire()
        postdata = urllib.urlencode(datas)
        headers={'User-Agent': self.__browserAgent,
                     'Referer': referer,
                     'Content-Type': 'application/x-www-form-urlencoded',
                     'Cache-Control': 'no-cache',
                     'Accept': '*/*',
                     'Connection': 'Keep-Alive',
                     'Accept-encoding':'gzip',
                     'Cookie':self.cookies}
        req = urllib2.Request(url=url,data=postdata,headers=headers)
        req.get_host()
        if timeout == '':
            open = urllib2.urlopen(req)
        else:
            open = urllib2.urlopen(req, timeout=timeout)
        if url != open.url:
            req = urllib2.Request(url=open.url,headers=headers)
        self._lock.release()
        return (open,req)

    def http_get(self, url ,refer='https://www.baidu.com'):
        return self.http_get_request(url, refer, 60)

    def http_post(self, url, datas ,refer='https://www.baidu.com'):
        return self.http_post_request(url, datas, refer, 60)


    def http_post_request2(self, url, datas, timeout=''):
        if timeout == '':
            open = urllib2.urlopen(url,datas)
        else:
            open = urllib2.urlopen(url, datas,timeout=timeout)
        data= open.read()
        return data


    def http_post2(self,url,datas):
        return self.http_post_request2(url,datas,300)


    def create_phandomjs(self, service_args, caps, timeout=30):
        self.driver = PhantomJS(desired_capabilities=caps,service_args = service_args)
        self.driver.set_page_load_timeout(timeout)
        self.driver.set_script_timeout(timeout)
        self.driver.implicitly_wait(timeout)

    def close_phandomjs(self):
        try:
            self.driver.quit()
        except:
            pass

    def http_get_phandomjs(self, url, refer='https://www.baidu.com', timeout=1000):
        caps = dict(DesiredCapabilities.PHANTOMJS)
        caps['browserName'] = 'chrome'
        caps["phantomjs.page.settings.resourceTimeout"] = timeout
        caps["phantomjs.page.settings.loadImages"] = False
        caps["phantomjs.page.settings.userAgent"] = (self.__browserAgent)
        caps["phantomjs.page.customHeaders.Referer"] = (refer)

        service_args=[]
        service_args.append('--load-images=no')
        service_args.append('--disk-cache=yes')
        service_args.append('--cookies-file=')

        self.create_phandomjs(timeout = timeout,service_args = service_args, caps = caps)
        self.driver.get(url)
        return self.driver.page_source

class SmartRedirectHandler(urllib2.HTTPRedirectHandler):
    def http_error_301(self, req, fp, code, msg, headers):
        result = urllib2.HTTPRedirectHandler.http_error_301(self, req, fp, code, msg, headers)
        result.status = code
        return result

    def http_error_302(self, req, fp, code, msg, headers):
        result = urllib2.HTTPRedirectHandler.http_error_302(self, req, fp, code, msg, headers)
        result.status = code
        return result

if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')
    r = RequestUtil()
    # rget = r.http_get('http://mil.news.sina.com.cn/china/2017-06-30/doc-ifyhrxsk1462288.shtml')
    # open = rget[0]
    # if open.info().get('Content-Encoding') == 'gzip':
    #     buf = StringIO(open.read())
    #     gz = gzip.GzipFile(fileobj=buf)
    #     html = gz.read()
    # else:
    #     html = open.read()
    #
    # html = html.decode('utf-8').encode(sys.getfilesystemencoding())
    # prit_str = html.replace("\r","").replace("\t","") + "\n"
    # print prit_str
    # f = FileUtil()
    # f.write_file_content_pattern("C:\user\hainiu\hainiu_cralwer\html1",prit_str,pattern="a")
    #
    # print prit_str
    html =  r.http_get_phandomjs('http://mil.news.sina.com.cn/china/2017-06-30/doc-ifyhrxsk1462288.shtml')
    html = html.decode('utf-8').encode(sys.getfilesystemencoding())
    print html
    r.close_phandomjs()