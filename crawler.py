#! /usr/bin/env python
# -*- coding: utf-8 -*-
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
import  json
import types
#from smallgfw import GFW
import os 
import os.path
from pymongo import ASCENDING,DESCENDING
import requests
from urlparse import urlparse
import sys
import urlparse
import re
import types 
mktime=lambda dt:time.mktime(dt.utctimetuple())
######################db.init######################
#connection = pymongo.Connection('localhost', 27017)
#
#post=kds.post
#
#browser = requests.session()
######################gfw.init######################
#gfw = GFW()
#gfw.set(open(os.path.join(os.path.dirname(__file__),'keyword.txt')).read().split('\n'))
#
#lgfw = GFW()
#lgfw.set(['thunder://','magnet:','ed2k://'])



def zp(data):
    """
    print dict list
    """
    for k in data:
        print '%s:'%k,data[k]

def get_html(url,referer ='',verbose=False):
    print '============================================'
    print 'url:',url
    print '============================================'
    time.sleep(1)
    html=''
    headers = ['Cache-control: max-age=0',]
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
        #crl.setopt(pycurl.HTTPHEADER,headers)
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
    tb搜索页爬虫
    """
    html=get_html(url)
    #print html
    if html:
        soup = BeautifulSoup(html,fromEncoding='gbk')
        items_row = soup.findAll('div',{'class':'row item icon-datalink'})
        if items_row:
            print '=======================row search row=========================='
            #print items
            for item in items_row:
                item_info = item.find('div',{'class':'col title'}).h3.a
                item_url = item_info['href']
                url_info = urlparse.urlparse(item_url)
                item_id = urlparse.parse_qs(url_info.query,True)['id'][0]
                print item_url
                print item_id
                judge_site(item_url)
        items_col = soup.findAll('div',{'class':'col item icon-datalink'})
        if items_col:
            print '=======================row search col=========================='
            #print items
            for item in items_col:
                item_info = item.find('div',{'class':'item-box'}).h3.a
                item_url = item_info['href']
                url_info = urlparse.urlparse(item_url)
                item_id = urlparse.parse_qs(url_info.query,True)['id'][0]
                print item_url
                print item_id
                judge_site(item_url)


def itemcrawler(iid,source='tb'):
    """
    tb物品页爬虫
    """
    if source == 'tb':
        url="http://item.taobao.com/item.htm?id=%s"%iid
    else:
        url="http://detail.tmall.com/item.htm?id=%s"%iid
        
    html=get_html(url)
    #print html
    if html:
        soup = BeautifulSoup(html,fromEncoding='gbk')
        shop_info = {}
        #请求销售数量url中需要的md5
        quantity_md5_patt = 'sbn=(\w{32})'
        qmd5 = re.findall(quantity_md5_patt,html)[0]
        for a in soup.find('meta',{'name':'microscope-data'})['content'].split(';'):
            k,v = a.strip().split('=')
            if k and v:
                shop_info[k] = int(v)
        price = soup.find('li',{'id':'J_StrPriceModBox'}).find('em',{'class':'tb-rmb-num'}).text
        #商品名称
        item_name = json.loads(soup.find('div',{'id':'J_itemViewed'})['data-value'])['title']
        shop_info['item_name'] = item_name
        #店铺名称
        shop_name = soup.find('a',{'class':'hCard fn'})['title']
        shop_info['shop_name'] = shop_name
        quantity_info = soup.find('li',{'class':'tb-sold-out tb-clearfix'})
        #有可能是个价格范围,先取最小值
        if '-' in price:
            shop_info['price'] = float(price.split('-')[0].strip())
        else:
            shop_info['price'] = float(price)
        shop_info['qmd5'] = qmd5
        #print 'shop_info:',shop_info
        return shop_info

def parse_price(iid,price,sellerid):
    """
    提取价格数据
    """
    data =  get_html('http://ajax.tbcdn.cn/json/umpStock.htm?itemId=%s&p=1&rcid=1&price=%s&sellerId=%s'%(iid,price,sellerid),referer='http://item.taobao.com/item.htm?id=%s'%iid,verbose=False)
    intkey = ['price','quanity','interval']
    resdict = {}
    data = data.decode('gbk').strip().replace('\r\n','').replace('\t','')
    patt_list = [r'price:\s*"(\w*\.\w*)"',
                 r'type:\s*"(.*)",\s*price' ,
                ]
    #print 'data:',data
    if re.findall(patt_list[0],data):
        real_price = float(re.findall(patt_list[0],data)[0])
        price_type = str(re.findall(patt_list[1],data)[0]) 
    else :
        #无活动,价格就是原价,或者是个价格范围
        real_price = price/100
        price_type = '无'
    return {'price':real_price,'type':price_type}

def parse_quantity(iid,sellerid,qmd5):
    """
    提取货物销量
    """
    url = "http://ajax.tbcdn.cn/json/ifq.htm?id=%s&sid=%s&p=1&ap=0&ss=0&free=0&q=1&ex=0&exs=0&at=b&ct=0&sbn=%s"%(iid,sellerid,qmd5)
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

def getTmallItemInfo(iid):
    """
    获取tm的物品信息
    """
    temp = {}
    patt_list = {
                #r""""sellerNickName"\s*:\s*(.*)'\s*,'isEcardAuction'""",
                'userid':r"'userId'\s*:\s*'(\w*)',",
                'shopid':r'rstShopId:(\w*),',
                'brand':r"'brand'\s*:\s*(.*)'\s*,'brandId'",
                'brandid':r"'brandId'\s*:\s*'(\w*)'", 
    }
    html = get_html("http://detail.tmall.com/item.htm?id=%s"%iid)
    #print 'html:',html
    htmlutf = html.replace('\r\n','').replace('\t','')
    soup = BeautifulSoup(html,fromEncoding='gbk')
    temp['itemname'] = soup.find('input',{'name':'title'})['value']
    temp['region'] = soup.find('input',{'name':'region'})['value']
    temp['sellername'] = soup.find('input',{'name':'seller_nickname'})['value']
    for k in patt_list:
        patt = patt_list[k]
        temp[k] = re.findall(patt,htmlutf)[0]
    url = "http://mdskip.taobao.com/core/initItemDetail.htm?tmallBuySupport=true&itemId=%s&service3C=true"%(iid)
    data = get_html(url,referer="http://detail.tmall.com/item.htm?id=%s"%iid).decode('gbk')#.replace('\r\n','').replace('\t','')
    patt = '"priceInfo":(\{.*\}),"promType"'
    price_info = re.findall(patt,data)
    if price_info:
        #print 'price_info:',price_info
        price_info = json.loads(price_info[0])
        if price_info.get('def'):
            temp['item_original_cost'] = float(price_info['def']['price'])
            if price_info['def']['promotionList']:
                temp['real_price'] = float(price_info['def']['promotionList'][0]['price'])
            else:
                temp['real_price'] = float(price_info['def']['tagPrice'])
        else:
            temp['item_original_cost'] = float(price_info[price_info.keys()[0]]['price'])
            temp['real_price'] = float(price_info[price_info.keys()[0]]['price'])
            
    patt = '"sellCountDO":(\{.*\}),"serviceDO"'
    quantity_info = re.findall(patt,data)
    if quantity_info:
        quantity_info = json.loads(quantity_info[0])
        temp['quantity'] = float(quantity_info['sellCount'])
    return temp


def getTaobaoItemInfo(iid):
    """
    获取tb物品页信息
    """
    item_original_info = itemcrawler(iid)
    price_info = parse_price(iid,int(item_original_info['price']*100),item_original_info['userid'])
    quantity_info = parse_quantity(iid,item_original_info['userid'],item_original_info['qmd5'])
    print '店名:',item_original_info['shop_name']
    print '物品名称:',item_original_info['item_name']
    print '物品原价:',item_original_info['price']
    if price_info:
        print '活动:',price_info['type']
        print '物品现价:',price_info['price']
    print '物品%s天内售出了%s件:'%(quantity_info['interval'],quantity_info['quanity'])

def judge_site(url):
    """
    判断物品是tb还是tm
    """
    url_info = urlparse.urlparse(url)
    urlkey = urlparse.parse_qs(url_info.query,True)
    iid = int(urlkey['id'][0])
    #print 'url_info:',url_info[1]
    if url_info[1] == 'detail.tmall.com':
        print 'it is a tm item'
        print getTmallItemInfo(iid)
    elif urlkey.get('cm_id'):
        print 'it is a tm item'
        print getTmallItemInfo(iid)
    else:
        print 'it is a tb item'
        print getTaobaoItemInfo(iid)

def getTmallShop(url):
    """
    获取tm商铺信息
    """
    html = get_html(url)
    if html:
        soup = BeautifulSoup(html,fromEncoding='gbk')
        hot_item_rank = soup.find('div',{'class':'rank-panels'})
        if hot_item_rank:
            hot_item_rank = hot_item_rank.div.ul.findAll('li')

if __name__ == "__main__":
    pass
    #print '*******************************************'
    #url = "http://mdskip.taobao.com/core/initItemDetail.htm?tmallBuySupport=true&itemId=15765842063&service3C=true"
    #data = get_html(url,referer="http://detail.tmall.com/item.htm?id=15765842063").decode('gbk').replace('\r\n','').replace('\t','')
    #patt = '.+?(\w+:\s*".*")'

    url = "http://s.taobao.com/search?q=无线键盘&commend=all&search_type=item&sourceId=tb.index"
    #searchcrawler(url)
    #print '*******************************************'
    #print res.decode('gbk')
    #print '+++++++++++++++++++++++++++++++++++++++++++++++++++++++='
    #print parse_quantity(15517664123)
    #print res['comments']
    #getTaobaoItemInfo(23290080040)
    #zp(getTmallItemInfo(10674262264))
    #print getTmallItemInfo(12434044828)
    #print parse_price(17824234211,6800)
    #print itemcrawler(17824234211)
    #judge_site('http://item.taobao.com/item.htm?id=14992324812&ad_id=&am_id=&cm_id=140105335569ed55e27b&pm_id=')
    print getTmallShop('logitech.tmall.com')



