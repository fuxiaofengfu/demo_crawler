#-*- encoding: utf-8 -*-
'''
example.py
Created on 2017/10/16 1:05
Copyright (c) 2017/10/16, 海牛学院版权所有.
@author: 青牛
'''
import mx.URL,sys
from tld import get_tld
from bs4 import BeautifulSoup
from commons.util.request_util import RequestUtil
from commons.util.html_util import HtmlUtil
from commons.util.util import Util

def test_beautiful():
    # url = 'http://roll.news.qq.com'
    url ='http://politics.gmw.cn/node_9844.htm'
    r = RequestUtil()
    hu = HtmlUtil()
    html = r.http_get_phandomjs(url)
    domain = get_tld(url)
    host = hu.get_url_host(url)
    u = Util()
    print "domain:",domain,":host:",host

    soup = BeautifulSoup(html, 'lxml')
    a_docs = soup.find_all("a")
    for a in a_docs:

        a_href = get_format_url(url,a,host)
        if a.text:
            print a.text
        if a_href:
            xpath = hu.get_dom_parent_xpath_js(a)

            print a_href,'_',xpath,u.get_md5(xpath)

def get_format_url(url,a_doc, host):
        a_href = a_doc.get('href')
        try:
            if a_href is not None and a_href.__len__() > 0:
                a_href = str(a_href).strip()
                a_href = a_href[:a_href.index('#')] if a_href.__contains__('#') else a_href
                # a_href = a_href.encode('utf8')
                # a_href = urllib.quote(a_href,safe='.:/?&=')
                if a_href.startswith('//'):
                    url = 'https:' + a_href if url.startswith('https:') else 'http:' + a_href
                    url = mx.URL.URL(str(url))
                    a_href = url.url
                elif a_href.startswith('/'):
                    url = 'https://' + host + a_href if url.startswith('https:') else 'http://' + host + a_href
                    url = mx.URL.URL(str(url))
                    a_href = url.url
                elif a_href.startswith('./') or a_href.startswith('../'):
                    url = mx.URL.URL(str(url) + '/' + a_href)
                    a_href = url.url
                elif not a_href.startswith('javascript') and not a_href.startswith('mailto') and not a_href.startswith('http') and a_href != '':
                    url = 'https://' + host + '/' + a_href if url.startswith('https:') else 'http://' + host + '/' + a_href
                    url = mx.URL.URL(str(url))
                    a_href = url.url
                a_href = a_href[:-1] if a_href.endswith('/') else a_href
                #a_href = a_href.lower()
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

if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf-8')
    test_beautiful()