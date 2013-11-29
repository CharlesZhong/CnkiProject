#!/usr/bin/python
# -*- coding: utf-8 -*-
# CnkiSpider.py crawl data from '*.cnki.com' And Store data into files
# author : zhongxin1990@gmail.com
import urllib2
from urllib2 import Request, urlopen, URLError, HTTPError
from urllib import urlencode
from bs4 import BeautifulSoup
import re
import MySQLdb
from time import  time
import os

class cnkiSpider():
    
    def __init__(self):
    	
    	self.author = ''
    	self.department = ''
        self.data = ''
        self.headers =  {
                #'Accept':'*/*',
                #'Accept-Encoding':'gzip,deflate,sdch',
                #'Accept-Language':'zh-CN,zh;q=0.8,en;q=0.6,zh-TW;q=0.4',
                'Connection':'keep-alive',
                'Cookie':'ASP.NET_SessionId=pfcjth3z04fl3uiydy5v3c45; LID=; LID=WEEvREcwSlJHSldTTGJhYlN2SDBwYy82emtTN0hmcVNmNW5qSDdxNjlGc096MzdTeDZFRFMybXhBVmJQRytKMQ==; SID_kns=120102; RsPerPage=50; cnkiUserKey=e4fd0059-f300-cb77-bcae-c122cf71ac25; CurTop10KeyWord=%2c%u5b59%u5bb6%u5e7f%2c%u6e05%u534e%u5927%u5b66',
                'Host':'epub.cnki.net',
                'Referer':'http://epub.cnki.net/KNS/brief/result.aspx?dbprefix=scdb&action=scdbsearch&db_opt=SCDB',
                'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.57 Safari/537.36'
                }
        self.keywordDir = "keyword/"
        self.paperDir = "paper/"
        self.index = 0
        #self.num = 0
    
    def session_con(self):
        try:
            baseurl = 'http://epub.cnki.net/KNS/request/SearchHandler.ashx?'
            url = baseurl + self.generate_Url(self.author,self.department,action='')
            req = urllib2.Request(url,self.data,self.headers)
            response = urllib2.urlopen(req)
            #sessionResult = response.read()   
        except urllib2.URLError, e:
            print e
        except HTTPError, e:
            print 'The server couldn\'t fulfill the request.'
            print 'Error code: ', e.code
        
        except Exception:
            print Exception
        #return sessionResult
    
    def keyword_con(self):
    	try:
    	    baseurl = 'http://epub.cnki.net/KNS/group/DoGroupLeft.ashx?'
            url = baseurl + self.generate_Url(self.author,self.department,action='44')
            req = urllib2.Request(url,self.data,self.headers)
            response = urllib2.urlopen(req)
            keywordResult = response.read()
            
            self.storeData(self.id,keywordResult,self.keywordDir)
        except urllib2.URLError, e:
            print e
        except HTTPError, e:
            print 'The server couldn\'t fulfill the request.'
            print 'Error code: ', e.code
        except Exception:
            print Exception
    	#return keywordResult
    
    def paper_con(self):
        try:
            #baseurl = 'http://epub.cnki.net/KNS/group/DoGroupLeft.ashx?'
            url =  'http://epub.cnki.net/KNS/brief/brief.aspx?pagename=ASP.brief_result_aspx&dbPrefix=SCDB&dbCatalog=%e4%b8%ad%e5%9b%bd%e5%ad%a6%e6%9c%af%e6%96%87%e7%8c%ae%e7%bd%91%e7%bb%9c%e5%87%ba%e7%89%88%e6%80%bb%e5%ba%93&ConfigFile=SCDB.xml&research=off&t=1385383351937&keyValue=&S=1'
            req = urllib2.Request(url,self.data,self.headers)
            response = urllib2.urlopen(req)
            PaperResult = response.read()
            total = self.parsePaperTotalNum(PaperResult)
            if total > 50:
            	self.headers['Cookie'] = 'ASP.NET_SessionId=pfcjth3z04fl3uiydy5v3c45; LID=; LID=WEEvREcwSlJHSldTTGJhYlN2SDBwYy82emtTN0hmcVNmNW5qSDdxNjlGc096MzdTeDZFRFMybXhBVmJQRytKMQ==; SID_kns=120102; RsPerPage='+total+'; cnkiUserKey=e4fd0059-f300-cb77-bcae-c122cf71ac25; CurTop10KeyWord=%2c%u5b59%u5bb6%u5e7f%2c%u6e05%u534e%u5927%u5b66'
            	req = urllib2.Request(url,self.data,self.headers)
            	response = urllib2.urlopen(req)
            	PaperResult = response.read() 
            
            self.storeData(self.id,PaperResult,self.paperDir)

        except urllib2.URLError, e:
            print e.reason
        except HTTPError, e:
            print 'The server couldn\'t fulfill the request.'
            print 'Error code: ', e.code
        except Exception:
            print Exception
        #return PaperResult

    def generate_Url(self,author,department,action):
    	if not isinstance(department,list) or len(department) == 1:
	    	param = {
					'action':action,
					'NaviCode':'*',
					'ua':'1.21',
					'PageName':'ASP.brief_result_aspx',
					'DbPrefix':'SCDB',
					'DbCatalog':'中国学术文献网络出版总库',
					'ConfigFile':'SCDB.xml',
					'db_opt':'CJFQ,CJFN,CDFD,CMFD,CPFD,IPFD,CCND,CCJD,HBRD',
					'base_special1':'%',
					'magazine_special1':'%',
					'au_1_sel':'AU',
					'au_1_sel2':'AF',
					'au_1_value1':author,
					'au_1_value2':department,
					'au_1_special1':'=',
					'au_1_special2':'%',
					'his':'0',
					'__':'Mon Nov 25 2013 19:39:40 GMT+0800 (CST)',
	    	}
    	elif len(department) == 2:
    		param = {
					'action':action,
					'NaviCode':'*',
					'ua':'1.21',
					'PageName':'ASP.brief_result_aspx',
					'DbPrefix':'SCDB',
					'DbCatalog':'中国学术文献网络出版总库',
					'ConfigFile':'SCDB.xml',
					'db_opt':'CJFQ,CJFN,CDFD,CMFD,CPFD,IPFD,CCND,CCJD,HBRD',
					'base_special1':'%',
					'magazine_special1':'%',
					'au_1_sel':'AU',
					'au_1_sel2':'AF',
					'au_1_value1':author,
					'au_1_value2':department[0],
					'au_1_special1':'=',
					'au_1_special2':'%',
					
					'au_2_sel':'AU',
					'au_2_sel2':'AF',
					'au_2_value1':author,
					'au_2_value2':department[1],
					'au_2_logical':'or',
					'au_2_special1':'=',
					'au_2_special2':'%',

					'his':'0',
					'__':'Mon Nov 25 2013 19:39:40 GMT+0800 (CST)',
	    	}
    	else:
    		param = {
					'action':action,
					'NaviCode':'*',
					'ua':'1.21',
					'PageName':'ASP.brief_result_aspx',
					'DbPrefix':'SCDB',
					'DbCatalog':'中国学术文献网络出版总库',
					'ConfigFile':'SCDB.xml',
					'db_opt':'CJFQ,CJFN,CDFD,CMFD,CPFD,IPFD,CCND,CCJD,HBRD',
					'base_special1':'%',
					'magazine_special1':'%',
					'au_1_sel':'AU',
					'au_1_sel2':'AF',
					'au_1_value1':author,
					'au_1_value2':department[0],
					'au_1_special1':'=',
					'au_1_special2':'%',
					
					'au_2_sel':'AU',
					'au_2_sel2':'AF',
					'au_2_value1':author,
					'au_2_value2':department[1],
					'au_2_logical':'or',
					'au_2_special1':'=',
					'au_2_special2':'%',

					'au_3_sel':'AU',
					'au_3_sel2':'AF',
					'au_3_value1':author,
					'au_3_value2':department[2],
					'au_3_logical':'or',
					'au_3_special1':'=',
					'au_3_special2':'%',

					'his':'0',
					'__':'Mon Nov 25 2013 19:39:40 GMT+0800 (CST)',
	    	}
    	url =  urlencode(param)
    	#print 80*'*'
        #print url
    	return url
    
    #  the total number of papers for An export
    def parsePaperTotalNum(self,paper_doc):
        soup = BeautifulSoup(paper_doc)
        result =  soup.find(class_='pagerTitleCell')
        total = filter(lambda x:x.isdigit(),str(result))
        return total
    # set params of an export 
    def setQueryParams(self,id,author,department):
    	self.id,self.author,self.department = id,author,self.translator(department)
    
    def storeData(self,id,doc,dir):
    	
    	f = open(os.path.join(dir,str(self.index))+'/'+str(id)+'_'+self.author+'_'+"".join(self.department)+'.html','w')
        #print os.path.join(dir,str(self.index))+'/'+str(id)+'_'+self.author+'_'+"".join(self.department)+'.html'
        f.write(doc)
        f.close()
        if dir == self.keywordDir:
        	print "keyword"+str(id)+"Success!",
        else :
        	print "paper"+str(id)+"Success!"
    
    def mkSubDir(self,index):
    	subKeywordDir = os.path.join(self.keywordDir,str(index))
    	if not os.path.isdir(subKeywordDir):
    		os.makedirs(subKeywordDir)
    	subPaperDir = os.path.join(self.paperDir,str(index))
    	if not os.path.isdir(subPaperDir):
    		os.makedirs(subPaperDir)
    	self.index = index
    
    def translator(self,department):
    	if department.find('-') != -1 :
    		return department.split('-')[0]
    	elif department.find('|') != -1:
    		return department.split('|')
    	elif department.find('、') != -1:
    		return department.split('、')
    	else:
    		return department
	#intab =  '-\/*'
	#outtab = '------'
	#trantab = maketrans(intab,outtab)
	return department.translate(None,'-') 

class DB():
	
	def __init__(self):
		self.conn = ""
		self.cursor = ""
	def connect(self):
		self.conn = MySQLdb.connect(host="localhost",user="root",passwd="123456",db="experts")
		self.cursor = self.conn.cursor()
	def close(self):
		self.cursor.close()
		self.conn.close()
	def find(self,begin,num):
		sql = "select SQL_CALC_FOUND_ROWS ZJ_NO,ZJ_NAME,DW_GUID  from newExpert order by ZJ_NO Limit "+str(begin)+","+str(num)
		n = self.cursor.execute(sql)
		#print n
		#print self.cursor.fetchall()
		return self.cursor.fetchall()
	def find_remain(self,begin,num):
		sql = "select SQL_CALC_FOUND_ROWS ZJ_NO,ZJ_NAME,DW_GUID  from newExpert order by ZJ_NO Limit "+str(begin)+","+str(num)
		n = self.cursor.execute(sql)
	def find_test(self,begin,num):
		sql = "select distinct DW_GUID  from newExpert  Limit "+str(begin)+","+str(num)
		n = self.cursor.execute(sql)
		#print n
		#print self.cursor.fetchall()
		return self.cursor.fetchall()




def main(start):
	scale = 1000
	spider = cnkiSpider()
	db = DB()

	
	for index in range((start/scale)+1,20):
		n = index*scale
		spider.mkSubDir(index)
		db.connect()
		expertSet = db.find(index*scale,scale)
		db.close()
		

		
		start = time()
		a = time()
		for AnExpert in expertSet:
			print "n: ",n,
			n = n+1	
			spider.setQueryParams(AnExpert[0],AnExpert[1],AnExpert[2])
			spider.session_con()
			spider.keyword_con()
			spider.paper_con()
			if n % 100 == 0:
				b = time()
				f = open("log.txt","a")
				f.write("total:"+str(n)+"\t"+str(b-a)+"\n")
				f.close()
				a = b
		end = time()

		print "time:",(end-start)

def main_remain(remainNum):
	#892
	#1785
	scale = 1000
	#remainNum = 892
	
	subDir = remainNum / scale
	
	spider = cnkiSpider()
	spider.mkSubDir(subDir)

	
		
	db = DB()	
	db.connect()
	
	expertSet = db.find(remainNum,scale-(remainNum%scale))
	#expertSet = db.find(1*scale,scale)
	db.close()
	
	n = remainNum
	start = time()
	a = time()
	for AnExpert in expertSet:
		print "n: ",n,
		n = n+1	
		spider.setQueryParams(AnExpert[0],AnExpert[1],AnExpert[2])
		spider.session_con()
		spider.keyword_con()
		spider.paper_con()
	
	end = time()
	
	print "time:",(end-start)
	'''
	for AnExpert in expertSet:
		for row in AnExpert:
			print row,
		print 
	print "total:",len(expertSet)
	print spider.index
	'''
	

if __name__ =="__main__":
	
	main_remain(17610)
	#main(17542)
      

