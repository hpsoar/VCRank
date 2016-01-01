__author__ = 'BorYee'

import re
import time
import urllib2

def _fetch(url):
    print '[INFO]', time.ctime(), 'Fetching', url
    src = None
    count = 0
    while True:
        try:
            src = str(urllib2.urlopen(url,timeout=5).read()).replace('\n', '').replace('\r', '')
            break
        except:
            count += 1
            print '[WARNING]', time.ctime(), count, 'Retry fetch', url
            if count > 5:
                break
    return src

def _filter_tag(src):
    src = src.replace('</td>','\t').replace(' ','')
    regex = '<.*?>'
    for tag in re.findall(regex,src):
        src= src.replace(tag,'')
    return src

def data_crawler():
    src_list = []
    res_list = []
    base_url = 'http://startupboard.sudoboot.com/contacts?from=groupmessage&isappinstalled=0&page='
    for index in range(1,74):
        src_list.append(_fetch(base_url +str(index)))
    regex = '<tr class="">(.*?)</tr>'
    for record in src_list:
        res_list.extend(re.findall(regex,record))
    with open('../res/startup.csv','w',-1) as f:
        for record in res_list:
            f.write(_filter_tag(record)+'\n')

if __name__ == '__main__':
    data_crawler()