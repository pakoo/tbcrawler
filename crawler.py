#! /usr/bin/env python
# -*- coding: utf-8 -*-
#Author:pako
#Email:zealzpc@gmail.com
"""
some db interface 
"""
import pymongo
import pycurl
from BeautifulSoup import BeautifulSoup 
import StringIO
import time
from django.utils.encoding import smart_str, smart_unicode
import os 
import traceback
import datetime
import gridfs
import simplejson as json
import types
from smallgfw import GFW
import os 
import os.path
from pymongo import ASCENDING,DESCENDING
import requests
from urlparse import urlparse
import sys
import urlparse
import re
mktime=lambda dt:time.mktime(dt.utctimetuple())
######################db.init######################
#connection = pymongo.Connection('localhost', 27017)
#
#kds=connection.kds
#post=kds.post
#kdsuser=kds.user
##fs=gridfs.GridFS(kds,'postfile')
#
#tieba = connection.tieba
#tieba_post = tieba.post
#tieba_user = tieba.user
#
#browser = requests.session()
######################gfw.init######################
gfw = GFW()
gfw.set(open(os.path.join(os.path.dirname(__file__),'keyword.txt')).read().split('\n'))

lgfw = GFW()
lgfw.set(['thunder://','magnet:','ed2k://'])




def get_html(url,referer ='',verbose=False):
    print '============================================'
    print 'url:',url
    print '============================================'
    time.sleep(1)
    html=''
    try:
        crl = pycurl.Curl()
        crl.setopt(pycurl.VERBOSE,1)
        crl.setopt(pycurl.FOLLOWLOCATION, 1)
        crl.setopt(pycurl.MAXREDIRS, 5)
        crl.setopt(pycurl.CONNECTTIMEOUT, 8)
        crl.setopt(pycurl.TIMEOUT, 30)
        crl.setopt(pycurl.VERBOSE, verbose)
        crl.setopt(pycurl.MAXREDIRS,10)
        crl.setopt(pycurl.USERAGENT,'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:9.0.1) Gecko/20100101 Firefox/9.0.1')
        if referer:
            crl.setopt(pycurl.REFERER,referer)
        crl.fp = StringIO.StringIO()
        crl.setopt(pycurl.URL, url)
        crl.setopt(crl.WRITEFUNCTION, crl.fp.write)
        crl.perform()
        html=crl.fp.getvalue()
        crl.close()
    except Exception,e:
        print('\n'*9)
        traceback.print_exc()
        print('\n'*9)
        return None
    return html

    #r = requests.get(url)
    #return r.text

    #r = browser.get(url)
    #return r.content

def transtime(stime):
    """
            将'11-12-13 11:30'类型的时间转换成unixtime
    """
    if stime and ':' in stime:
        res=stime.split(' ')
        year,mon,day=[int(i) for i in res[0].split('-')]
        hour,second=[int(i) for i in res[1].split(':')]
        unixtime=mktime(datetime.datetime(year,mon,day,hour,second))
        return unixtime
    else:
        return int(time.time())


def searchcrawler(url):
    """
    淘宝搜索页爬虫
    """
    html=get_html(url)
    #print html
    if html:
        soup = BeautifulSoup(html,fromEncoding='gbk')
        items = soup.findAll('div',{'class':'col item icon-datalink'})
        print 'items len:',len(items)
        print '==================================================='
        #print items[0]
        for item in items:
            item_info = item.find('div',{'class':'item-box'}).h3.a
            item_url = item_info['href']
            url_info = urlparse.urlparse(item_url)
            item_id = urlparse.parse_qs(url_info.query,True)['id'][0]
            print item_url
            print item_id


def itemcrawler(iid):
    """
    淘宝物品页爬虫
    """
    url="http://item.taobao.com/item.htm?&id=%s"%iid
    html=get_html(url)
    #print html
    if html:
        soup = BeautifulSoup(html,fromEncoding='gbk')
        price = soup.find('li',{'id':'J_StrPriceModBox'}).find('em',{'class':'tb-rmb-num'}).text
        quantity_info = soup.find('li',{'class':'tb-sold-out tb-clearfix'})
        print 'price:',price
        return float(price)

def parse_price(iid,price):
    """
    提取价格数据
    """
    data =  get_html('http://ajax.tbcdn.cn/json/umpStock.htm?itemId=%s&p=1&rcid=1&price=%s&sellerId=6'%(iid,price),'http://item.taobao.com/item.htm?id=%s'%iid)
    intkey = ['price','quanity','interval']
    resdict = {}
    data = data.decode('gbk').strip().replace('\r\n','').replace('\t','')
    patt = '.+?(\w+:\s*".*")'
    res = re.match(patt,data)
    if res:
        res = res.groups()[0].split(',')
        for r in res:
            key,value = r.split(':')
            key = key.strip() 
            value=eval(value.strip())
            #print 'key:',key
            #print 'value:',value
            if key in intkey:
                value = float(value)
            resdict[key] = value
    return resdict

def parse_quantity(iid):
    """
    提取货物销量
    """
    url = "http://ajax.tbcdn.cn/json/ifq.htm?id=%s&sid=1&p=1&ap=0&ss=0&free=0&q=1&ex=0&exs=0&at=b&ct=0"%iid
    data = get_html(url)
    intkey = ['quanity','interval']
    resdict = {}
    data = data.decode('gbk').strip().replace('\r\n','').replace('\t','')
    patt_list = [
                r'interval:\s*\w*',
                r'quanity:\s*\w*\.?\w*',
                r'location:\s*\'.*\',',
                r'carriage:\s*\'.*\'',
                ]

    complie_list = [re.compile(a) for a in patt_list]
    for c in complie_list:
        res = re.findall(c,data)
        if res:
            res= res[0]
            for r in res.split(','):
                if r:
                    key,value = r.split(':',1)
                    key = key.strip()
                    value = value.strip()
                    if key in intkey:
                        value = float(value)
                    resdict[key]= value
    return resdict

def get_item_info(iid,source='tb'):
    """
    获取物品页信息
    """
    item_original_cost = itemcrawler(iid)
    price_info = parse_price(iid,int(item_original_cost*100))
    quantity_info = parse_quantity(iid)
    print '物品原价:',item_original_cost
    if price_info:
        print price_info['type']
        print '物品现价:',price_info['price']
    print '物品%s天内售出了%s件:'%(quantity_info['interval'],quantity_info['quanity'])

if __name__ == "__main__":
    pass
    #print '*******************************************'
    #url = "http://s.taobao.com/search?q=无线路由器&commend=all"
    #searchcrawler(url)
    #print '*******************************************'
    #itemcrawler(15517664123)
    #print res.decode('gbk')
    #print parse_price(15517664123,28900)
    #print '+++++++++++++++++++++++++++++++++++++++++++++++++++++++='
    #print parse_quantity(15517664123)
    get_item_info(13806634536)



