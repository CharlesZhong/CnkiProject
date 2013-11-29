#!/usr/bin/python
# -*- coding: utf-8 -*-

# a web spider to crawl data in http://kjpj.cutech.edu.cn/zjk/Sys/User/Login.aspx
# author : xiaoh16@gmail.com

import json
import urllib2
from urllib2 import Request, urlopen, URLError, HTTPError


class Spider():
    def __init__(self,page_dir,sessionId, timeout=20):
        self.timeout = timeout
        self.page_dir = page_dir
        self.count = 0
        self.base_url =  'http://kjpj.cutech.edu.cn/zjk/Sys/'
        self.GUID = '00E41376-EED5-04F2-B462-1A83848EDD2D'
        self.sessionId = sessionId

    def crawl_expert_list(self):
        while(self.count < 81):
            try:
                data = '<document><Search><ROWNUM-MAX>'+((self.count+1)*1000).__str__()+'</ROWNUM-MAX><ROWNUM-MIN>'+(self.count*1000).__str__()+'</ROWNUM-MIN></Search></document>'
                headers = {
                    'Accept':'*/*',
                    'Content-Type':' text/xml; charset=utf-8',
                    'DATA-FORMAT':'XML',
                    'Referer':'http://kjpj.cutech.edu.cn/zjk/Sys/List/List.aspx?GUID='+self.GUID,
                    'Accept-Language':'zh-cn',
                    'Accept-Encoding':'gzip, deflate',
                    'User-Agent':' Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.2; WOW64; Trident/6.0; .NET4.0E; .NET4.0C)',
                    'Host':'kjpj.cutech.edu.cn',
                    'DNT':'1',
                    'Content-Length':len(data),
                    'Connection':'Keep-Alive',
                    'Cache-Control':'no-cache',
                    'Cookie':'ASP.NET_SessionId='+self.sessionId
                }
                url = self.base_url+'List/Search.aspx?GUID='+self.GUID
                req = urllib2.Request(url,data,headers)
                response = urllib2.urlopen(req)
                html = response.read()
                html = html.decode("gb2312",'ignore').encode('utf-8')
                f = open(self.page_dir+self.count.__str__()+'.html','w')
                f.write(html)
                f.close()
                self.count = self.count + 1
            except urllib2.URLError, e:
                print e.reason
            except HTTPError, e:
                print 'The server couldn\'t fulfill the request.'
                print 'Error code: ', e.code
            except UnicodeEncodeError,e:
                print e

if __name__ == '__main__':
    spider = Spider(page_dir='pages/',sessionId='vz1je21bo53a5s3ocriqgc34')
    spider.crawl_expert_list()