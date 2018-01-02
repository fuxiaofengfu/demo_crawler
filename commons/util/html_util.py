#-*- encoding: utf-8 -*-
'''
Created on 2017/7/1 13:49
Copyright (c) 2017/7/1, 海牛学院版权所有.
@author: 青牛
'''
import urllib
class HtmlUtil:

    def get_doc_charset(self,doc):
        #<meta charset="UTF-8" />
        #<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
        charset = 'utf-8'
        meta = doc.xpath('//meta[@charset]')
        if meta and len(meta) > 0:
            charset = meta[0].attrib.get('charset',charset)
        else:
            meta = doc.xpath("//meta[@http-equiv='Content-Type']")
            if meta and len(meta) > 0:
                content = meta[0].attrib.get('content','')
                if content:
                    p = content.find('charset=')
                    if p > 0:
                        charset=content[p + len('charset='):]
        return charset

    def get_dom_parent_xpath(self,dom):
        parents = []
        p = dom
        while True:
            if p is None:
                break
            #print p.attrib
            parents.append(p)
            if p.attrib.get('id',None):
                break
            p = p.getparent()

        xpath = ['/']
        for p in reversed(parents):
            id_name = p.attrib.get('id',None)
            class_name = p.attrib.get('class',None)
            if id_name:
                xpath.append('/')
                xpath.append(p.tag)
                xpath.append('[@id=\'')
                xpath.append(id_name)
                xpath.append('\']')
            elif class_name:
                xpath.append('/')
                xpath.append(p.tag)
                xpath.append('[contains(@class,\'')
                xpath.append(class_name)
                xpath.append('\')]')
            else:
                xpath.append('/')
                xpath.append(p.tag)

        return "".join(xpath)

    def get_dom_parent_xpath_js(self,dom):
        parents = []
        p = dom
        while True:
            if p is None:
                break
            #print p.attrib
            parents.append(p)
            if p.get('id',None):
                break
            p = p.parent

        xpath = ['/']
        for p in reversed(parents):
            id_name = p.get('id',None)
            class_name = p.get('class',None)
            if id_name:
                xpath.append('/')
                xpath.append(p.name)
                xpath.append('[@id=\'')
                xpath.append(id_name)
                xpath.append('\']')
            elif class_name:
                xpath.append('/')
                xpath.append(p.name)
                xpath.append('[contains(@class,\'')
                xpath.append(class_name[0])
                xpath.append('\')]')
            else:
                xpath.append('/')
                xpath.append(p.name)

        return "".join(xpath)

    def get_url_host(self, url):
        s1 = urllib.splittype(url)[1]
        return urllib.splithost(s1)[0]