# coding=utf-8
__author__ = 'BorYee'

import re
import math
import json
import time
import urllib2
import datetime
import MySQLdb
from cookielib import MozillaCookieJar

# ########## GLOBAL ####### #
UPDATE_GAP = 2
CURRENT_DATE = time.strftime('%Y%m%d', time.localtime(time.time()))
CONN = MySQLdb.connect(host='localhost', user='root', db='db_angelcrunch')
HEADERS = [
    ('User-Agent',
     'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.90 Safari/537.36')
    ,
    ('Referer', 'http://13112820.angelcrunch.com/')
]
cookie_jar = MozillaCookieJar()
opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cookie_jar))
# ########## GLOBAL ####### #

def _login():
    url = 'http://auth.angelcrunch.com/j/login?callback=jQuery111203010284334886819_1449729376948&o=%7B%22account%22%3A%22cyu3%40qq.com%22%2C%22password%22%3A%22vchello168%22%7D&_=1449729376950'
    opener.addheaders = HEADERS
    opener.open(url).read()


def _fetch(url):
    print '[INFO]', time.ctime(), 'Fetching', url
    src = None
    count = 0
    while True:
        try:
            src = str(opener.open(url).read()).replace('\n', '').replace('\r', '')
            break
        except:
            count += 1
            print '[WARNING]', time.ctime(), count, 'Retry fetch', url
            if count > 5:
                break
    return src


def _filter(src):
    return src.replace('\n', '')


def get_investor_base_info():
    cursor = CONN.cursor()
    src_json_list = []
    base_url = 'http://angelcrunch.com/j/angel?business=%E5%85%A8%E9%83%A8&collection=all&area=%E5%85%A8%E9%83%A8&limit=15&page='
    src = _fetch(base_url + '1')
    src = json.loads(src)
    src_json_list.append(src)
    total_investor_number = src[2]
    page_size = int(math.ceil(total_investor_number / 15.0))
    for index in range(2, page_size + 1):
        src = _fetch(base_url + str(index))
        src_json_list.append(json.loads(src))
    judge_sql = 'select * from tb_investor_base_info where investor_name = %s and investor_link = %s'
    insert_sql = 'insert into tb_investor_base_info (investor_name,investor_link,address,business,company,title,txt) VALUES (%s,%s,%s,%s,%s,%s,%s)'
    for src in src_json_list:
        for record in src[0]:
            info = []
            if record.__len__() > 10:
                info.append(record['name'])
                info.append(record['user_page'])
                info.append(record['address'])
                info.append(record['business'])
                info.append(record['company'])
                info.append(record['title'])
                info.append(record['txt'])
                info = map(lambda x: x.replace('\n', '').replace('\r', ''), info)
                if not cursor.execute(judge_sql, [info[0], info[1]]):
                    cursor.execute(insert_sql, info)
                    print '[INFO]', time.ctime(), 'Executed', insert_sql % tuple(info)
    cursor.close()
    CONN.commit()


def get_investot_detail_info(url):
    '''
    This function is using for crawling and extracting the investor detail info.
    The info include:
        name
        weibo
        weixin
        company
        title
        linkedin
        mail
        phone
        school
        degree
        investment_idea
        skilled_area
            list : String
        added_value
            list : String
        intrested_location
            list : String
        max_amount
        min_amount
        intrested_area
            list : String
        investment_plan
        exp
            list : dict
                com_name
                role
                round
                amount
                valuation
        follow_project
            list : dict
                name
                com_id

    :param url: String
        The investor detail info url
    '''
    detail_info = {}
    src = _fetch('http:' + url)
    if not src == None:
        regex = 'render_basic_info[(]{(.*?)}[)]'
        basic_info = re.findall(regex, src)
        if basic_info.__len__() > 0 and basic_info[0].__len__() > 5:
            basic_info = '{' + basic_info[0] + '}'
            basic_info = json.loads(basic_info)
            detail_info['name'] = _filter(basic_info['name'])
            detail_info['weibo'] = basic_info['weibo']
            detail_info['weixin'] = basic_info['weixin']
            detail_info['company'] = _filter(basic_info['company'])
            detail_info['title'] = _filter(basic_info['title'])
            detail_info['linkedin'] = basic_info['linkedin']
            detail_info['mail'] = basic_info['mail']
            detail_info['phone'] = basic_info['phone']
        regex = 'render_edu[(][[](.*?)[]][)]'
        edu = re.findall(regex, src)
        if edu.__len__() > 0 and edu[0].__len__() > 5:
            edu = json.loads('[' + edu[0] + ']')[0]
            detail_info['school'] = _filter(edu['school'])
            detail_info['degree'] = _filter(edu['degree'])
        regex = 'render_style[(]{(.*?)}[)]'
        style = re.findall(regex, src)
        if style.__len__() > 0 and style[0].__len__() > 5:
            style = json.loads('{' + style[0] + '}')
            detail_info['investment_idea'] = _filter(style['style'])
            detail_info['skilled_area'] = _split(_filter(style['tag']), u'\u00b7')
            detail_info['added_value'] = _split(_filter(style['resource']), u'\u00b7')
        regex = 'render_invest_plan[(]{(.*?)}[)]'
        plan = re.findall(regex, src)
        if plan.__len__() > 0 and plan[0].__len__() > 5:
            plan = json.loads('{' + plan[0] + '}')
            if not _get_attr(plan, 'region') == '':
                detail_info['intrested_location'] = _split(_filter(plan['region']), u'\u00b7')
            else:
                detail_info['intrested_location'] = []
            if not _get_attr(plan, 'max_amount') == '':
                detail_info['max_amount'] = str(plan['max_amount'])
            else:
                detail_info['max_amount'] = ''
            if not _get_attr(plan, 'min_amount') == '':
                detail_info['min_amount'] = str(plan['min_amount'])
            else:
                detail_info['min_amount'] = ''
            if not _get_attr(plan, 'business') == '':
                detail_info['intrested_area'] = _split(_filter(plan['business']), u'\u00b7')
            else:
                detail_info['intrested_area'] = []
            if not _get_attr(plan, 'count_') == '':
                try:
                    detail_info['investment_plan'] = str(plan['count_'])
                except:
                    detail_info['investment_plan'] = plan['count_']
            else:
                detail_info['investment_plan'] = ''
        regex = 'render_exp[(] \[(.*?)\]'
        exp = re.findall(regex, src)
        if exp.__len__() > 0 and exp[0].__len__() > 5:
            round_index = [
                u'种子',
                u'天使',
                u'A',
                u'B',
                u'C',
                u'D',
                u'快速合投',
                u''
            ]
            role_index = [
                u'职员',
                u'高管',
                u'创始人',
                u'投资人',
                u'顾问导师',
                u'律师',
                u''
            ]
            exp_list = []
            exp = json.loads('[' + exp[0] + ']')
            for record in exp:
                exp_record_dict = {}
                exp_record_dict['com_name'] = record['com_name']
                if not _get_attr(record, 'role') == '':
                    exp_record_dict['role'] = role_index[int(record['role']) - 1]
                else:
                    exp_record_dict['role'] = ''
                if record['role'] == 4:
                    if not _get_attr(record, 'stage') == '':
                        exp_record_dict['round'] = round_index[int(record['stage']) - 1]
                    else:
                        exp_record_dict['round'] = ''
                    if not _get_attr(record, 'amount') == '':
                        exp_record_dict['amount'] = str(record['amount'])
                    else:
                        exp_record_dict['amount'] = ''
                    if not _get_attr(record, 'valuation') == '':
                        exp_record_dict['valuation'] = str(record['valuation'])
                    else:
                        exp_record_dict['valuation'] = ''
                exp_list.append(exp_record_dict)
            detail_info['exp'] = exp_list
        intrested_project_url = 'http:' + url + '/j/following_com?offset=0&limit=100'
        follow_list = json.loads(_fetch(intrested_project_url))
        if 'com_list' in follow_list:
            follow_com = []
            for record in follow_list['com_list']:
                follow_com_record = {}
                follow_com_record['name'] = record['name']
                follow_com_record['com_id'] = str(record['com_id'])
                follow_com.append(follow_com_record)
            detail_info['follow_project'] = follow_com
        cursor = CONN.cursor()
        judege_sql = 'SELECT * FROM db_angelcrunch.tb_investor_detail_info where investor_name = %s and company = %s'
        deatil_info_insert_sql = 'insert into tb_investor_detail_info (investor_name,degree,school,investment_ideas,investment_plan,min_amount,max_amount,mail,weixin,phone,linkedin,company,title) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        skilled_area_insert_sql = 'insert into tb_investor_skilled_area (investor_name,area) values(%s,%s)'
        intrested_area_insert_sql = 'insert into tb_investor_intrested_area (investor_name,area) values(%s,%s)'
        intrested_location_insert_sql = 'insert into tb_investor_intrested_location (investor_name,location) values(%s,%s)'
        added_value_insert_sql = 'insert into tb_investor_added_value (investor_name,added_value) values(%s,%s)'
        exp_insert_sql = 'insert into tb_investor_experience (investor_name,project_name,project_type,round,amount,valuation) values(%s,%s,%s,%s,%s,%s)'
        follow_project_insert_sql = 'insert into tb_investor_follow_project (investor_name,project_name,project_link) values(%s,%s,%s)'
        update_sql = 'update tb_investor_base_info set update_time = %s where investor_link = %s'
        if not cursor.execute(judege_sql, [_get_attr(detail_info, 'name'), _get_attr(detail_info, 'company')]):
            tmp = []
            tmp.append(_get_attr(detail_info, 'name'))
            tmp.append(_get_attr(detail_info, 'degree'))
            tmp.append(_get_attr(detail_info, 'school'))
            tmp.append(_get_attr(detail_info, 'investment_idea'))
            tmp.append(_get_attr(detail_info, 'investment_plan'))
            tmp.append(_get_attr(detail_info, 'min_amount'))
            tmp.append(_get_attr(detail_info, 'max_amount'))
            tmp.append(_get_attr(detail_info, 'mail'))
            tmp.append(_get_attr(detail_info, 'weixin'))
            tmp.append(_get_attr(detail_info, 'phone'))
            tmp.append(_get_attr(detail_info, 'linkedin'))
            tmp.append(_get_attr(detail_info, 'company'))
            tmp.append(_get_attr(detail_info, 'title'))
            cursor.execute(deatil_info_insert_sql, tmp)
            print '[INFO]', time.ctime(), 'Executed', deatil_info_insert_sql % tuple(tmp)
            if not _get_attr(detail_info, 'skilled_area') == '':
                for record in _get_attr(detail_info, 'skilled_area'):
                    cursor.execute(skilled_area_insert_sql, [_get_attr(detail_info, 'name'), record])
                    print '[INFO]', time.ctime(), 'Executed', skilled_area_insert_sql % tuple(
                        [_get_attr(detail_info, 'name'), record])
            if not _get_attr(detail_info, 'intrested_area') == '':
                for record in _get_attr(detail_info, 'intrested_area'):
                    cursor.execute(intrested_area_insert_sql, [_get_attr(detail_info, 'name'), record])
                    print '[INFO]', time.ctime(), 'Executed', intrested_area_insert_sql % tuple(
                        [_get_attr(detail_info, 'name'), record])
            if not _get_attr(detail_info, 'intrested_location') == '':
                for record in _get_attr(detail_info, 'intrested_location'):
                    cursor.execute(intrested_location_insert_sql, [_get_attr(detail_info, 'name'), record])
                    print '[INFO]', time.ctime(), 'Executed', intrested_location_insert_sql % tuple(
                        [_get_attr(detail_info, 'name'), record])
            if not _get_attr(detail_info, 'added_value') == '':
                for record in _get_attr(detail_info, 'added_value'):
                    cursor.execute(added_value_insert_sql, [_get_attr(detail_info, 'name'), record])
                    print '[INFO]', time.ctime(), 'Executed', added_value_insert_sql % tuple(
                        [_get_attr(detail_info, 'name'), record])
            if not _get_attr(detail_info, 'exp') == '':
                for record in _get_attr(detail_info, 'exp'):
                    sub_tmp = []
                    sub_tmp.append(_get_attr(detail_info, 'name'))
                    sub_tmp.append(_get_attr(record, 'com_name'))
                    sub_tmp.append(_get_attr(record, 'role'))
                    sub_tmp.append(_get_attr(record, 'round'))
                    sub_tmp.append(_get_attr(record, 'amount'))
                    sub_tmp.append(_get_attr(record, 'valuation'))
                    cursor.execute(exp_insert_sql, sub_tmp)
                    print '[INFO]', time.ctime(), 'Executed', exp_insert_sql % tuple(sub_tmp)
            if not _get_attr(detail_info, 'follow_project') == '':
                for record in _get_attr(detail_info, 'follow_project'):
                    sub_tmp = []
                    sub_tmp.append(_get_attr(detail_info, 'name'))
                    sub_tmp.append(_get_attr(record, 'name'))
                    sub_tmp.append(_get_attr(record, 'com_id'))
                    cursor.execute(follow_project_insert_sql, sub_tmp)
                    print '[INFO]', time.ctime(), 'Executed', follow_project_insert_sql % tuple(sub_tmp)
            cursor.execute(update_sql, [CURRENT_DATE, url])
            print '[INFO]', time.ctime(), 'Executed', update_sql % tuple([CURRENT_DATE, url])
            cursor.close()


def _split(src, seg):
    if not src.find(src) == -1:
        src = src.split(seg)
    else:
        src = [src]
    return src


def _get_attr(dic, key):
    if key in dic:
        return dic[key]
    else:
        return ''


def investor_detail_info_control():
    cursor = CONN.cursor()
    sql = 'select * from tb_investor_base_info'
    for record in cursor.fetchmany(cursor.execute(sql)):
        if record[8] == None:
            get_investot_detail_info(record[2])
        else:
            curr_time = time.strptime(CURRENT_DATE, '%Y%m%d')
            curr_time = datetime.datetime(curr_time[0], curr_time[1], curr_time[2])
            last_update_time = time.strptime(record[8], '%Y%m%d')
            last_update_time = datetime.datetime(last_update_time[0], last_update_time[1], last_update_time[2])
            delta_time = (curr_time - last_update_time).days
            if delta_time > UPDATE_GAP:
                get_investot_detail_info(record[2])
    cursor.close()


def export_data_to_excel():
    cursor = CONN.cursor()
    sql = 'SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = %s'
    select_sql = 'select * from '
    for table in cursor.fetchmany(cursor.execute(sql, ['db_angelcrunch'])):
        with open('../res/'+table[0]+'.csv','w',-1) as f:
            for record in cursor.fetchmany(cursor.execute(select_sql + table[0])):
                f.write('\t'.join(map(str,record)) + '\n')
    cursor.close()


if __name__ == '__main__':
    try:
        # _login()
        # # get_investor_base_info()
        # investor_detail_info_control()
        export_data_to_excel()
    finally:
        CONN.commit()
        CONN.close()