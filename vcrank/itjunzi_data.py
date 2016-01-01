# coding=utf-8
__author__ = 'BorYee'

import MySQLdb
import urllib
import urllib2
import re
import time
import random
import datetime
from cookielib import MozillaCookieJar
from invest_relation import build_relation
from invest_firm_rank import build_rank

########## GLOBAL PARAMS #######
UPDATE_GAP = 10
CURRENT_DATE = time.strftime('%Y%m%d', time.localtime(time.time()))
HOST = 'localhost'
USER = 'root'
PASSWD = 'root'
DB = 'db_investment'
CONN = MySQLdb.connect(host=HOST, user=USER, db=DB)
HEADERS = [
    ('User-Agent',
     'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.90 Safari/537.36')
]
cookie_jar = MozillaCookieJar()
opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cookie_jar))
########## GLOBAL PARAMS #######

def _filter_html_tag(src):
    """
    This function is using for filtering the html tag.

    :param src: string
        Source file string

    :return src: string
        Filtered string
    """
    regex = '<.*?>'
    res_list = re.findall(regex, src)
    for res in res_list:
        src = src.replace(res, '')
    return src.strip()


def _login():
    """
    This function is using for login in itjuzi.com by program.

    """
    login_url = 'http://itjuzi.com/user/ajax/login'
    post_data = {
        'uname': 'boryee@163.com',
        'upwd': '911007',
        'remember': 'true'
    }
    post_data = urllib.urlencode(post_data)
    opener.open(login_url, post_data).read()
    opener.addheaders = HEADERS


def _fetch(url):
    """
    This function is using for fetching the html source.

    :param url: string
        The url.
    :return src: string
        The html source.
    """
    src = opener.open(url, timeout=10).read()
    src = str(src).replace('\n', '')
    return src


def _wait():
    """
    The timer function.

    """
    sleep_time = 2 * random.random()
    print '[INFO]', time.ctime(), 'Now sleep', sleep_time, 'seconds!'
    time.sleep(sleep_time)
    print '[INFO]', time.ctime(), 'Finish sleeping!'


def _res_judge(res):
    if res.__len__() > 0:
        return _filter_html_tag(res[0])
    else:
        return ''


def get_invest_firm_link():
    """
    This function is using for fetching and extracting the invest firm link from itjuzi.com.
    """
    base_url = 'http://itjuzi.com/investfirm?page='
    initial_url = 'http://itjuzi.com/investfirm'
    initial_page = _fetch(initial_url)
    regex = 'http://itjuzi.com/investfirm[?]page=(.*?)"'
    res_list = re.findall(regex, initial_page)
    page_number = max(map(int, res_list))
    src_list = ['' for i in range(page_number)]
    src_list[0] = initial_page
    for i in range(2, page_number + 1):
        try:
            url = base_url + str(i)
            src_list[i - 1] = _fetch(url)
            print '[INFO]', time.ctime(), 'Finish crawling', url, 'with size', len(src_list[i - 1])
        except:
            i -= 1
            _wait()
            continue
    name_regex = '名称:(.*?)</a>'
    link_regex = '名称:  <a href="(.*?)">'
    link_list = []
    name_list = []
    for src in src_list:
        res_list = re.findall(name_regex, src)
        sub_name_list = map(_filter_html_tag, res_list)
        sub_link_list = re.findall(link_regex, src)
        link_list.extend(sub_link_list)
        name_list.extend(sub_name_list)
    name_list = map(lambda x: x.strip(), name_list)
    link_list = map(lambda x: x.strip(), link_list)
    select_sql = 'SELECT * FROM db_investment.tb_invest_firm_link WHERE invest_firm_name like %s'
    insert_sql = 'INSERT INTO db_investment.tb_invest_firm_link (invest_firm_name,invest_firm_link) VALUES (%s,%s)'
    cursor = CONN.cursor()
    for i in range(name_list.__len__()):
        if not cursor.execute(select_sql, ['%' + name_list[i] + '%']):
            cursor.execute(insert_sql, (name_list[i], link_list[i]))
            print '[INFO]', time.ctime(), 'Insert invest firm', name_list[i]
    cursor.close()


def get_invest_firm_info(name, url):
    """
    This function is using for fetching, extracting and storing the info of the invest firm.

    :param name: String
        The invest firm name.
    :param url: String
        The url of the invest firm.
    """
    src = _fetch(url)
    base_info = []
    stage = []
    area = []
    events = []
    events_link = []
    investors_link = []
    regex = '<div class="public-info pull-left">(.*?)</div>'
    res = re.findall(regex, src)
    if res.__len__() > 0:
        name = _filter_html_tag(res[0])
        base_info.append(name)
    else:
        base_info.append('')
    regex = '<ul class="detail-info">(.*?)</ul>'
    res = re.findall(regex, src)
    if res.__len__() > 0:
        res = res[0]
        regex = '网址:  (.*?)</a>'
        link = re.findall(regex, res)
        if link.__len__ > 0:
            link = _filter_html_tag(link[0])
            base_info.append(link)
        else:
            base_info.append('')
        regex = '阶段:  (.*?)</li>'
        stage = re.findall(regex, res)
        if stage.__len__() > 0:
            stage = _filter_html_tag(stage[0])
            stage = stage.split(',')
            stage = map(lambda x: x.strip(), stage)
        else:
            stage = []
        regex = '领域:  (.*?)</li>'
        area = re.findall(regex, res)
        if area.__len__() > 0:
            area = _filter_html_tag(area[0])
            area = area.split(',')
            area = map(lambda x: x.strip(), area)
        else:
            area = []
        regex = '介绍:  (.*?)</li>'
        intro = re.findall(regex, res)
        if intro.__len__() > 0:
            intro = _filter_html_tag(intro[0])
            base_info.append(intro)
        else:
            base_info.append('')
    regex = '<tbody>(.*?)</tbody>'
    res = re.findall(regex, src)
    if res.__len__() > 0:
        res = res[0]
        regex = '<tr>(.*?)</tr>'
        event_src = re.findall(regex, res)
        if event_src.__len__() > 0:
            for event in event_src:
                regex = '<td.*?</td>'
                event_info = re.findall(regex, event)
                event_info = map(_filter_html_tag, event_info)
                events.append(event_info)
                regex = '<a href="(.*?)"'
                event_link = re.findall(regex, event)
                if event_link.__len__() > 0:
                    events_link.append((event_info[1], event_link[0]))
    regex = '<div class="person-list clearfix side-bar-person">(.*?)<footer id="page-footer" class="dark clearfix">'
    res = re.findall(regex, src)
    if res.__len__() > 0:
        res = res[0]
        regex = '<h4.*?</h4>'
        investor_src = re.findall(regex, res)
        if investor_src.__len__() > 0:
            regex = 'a href="(.*?)"'
            for investor in investor_src:
                investor_link = re.findall(regex, investor)[0]
                investor_name = _filter_html_tag(investor)
                investors_link.append((investor_name, investor_link))
    for record in events:
        print ','.join(record)
    cursor = CONN.cursor()
    judge_sql = 'select * from tb_invest_firm_base_info where invest_firm_name = %s'
    if not cursor.execute(judge_sql, [base_info[0]]):
        sql = 'insert into tb_invest_firm_base_info (invest_firm_name, official_link,intro) values(%s,%s,%s)'
        cursor.execute(sql, base_info)
        print '[INFO]', time.ctime(), 'Executed', sql % tuple(base_info)
    judge_sql = 'select * from tb_invest_firm_stage where invest_firm_name = %s and stage = %s'
    sql = 'insert into tb_invest_firm_stage (invest_firm_name,stage) values(%s,%s)'
    for value in stage:
        if not cursor.execute(judge_sql, [base_info[0], value]):
            cursor.execute(sql, [base_info[0], value])
            print '[INFO]', time.ctime(), 'Executed', sql % tuple([base_info[0], value])
    judge_sql = 'select * from tb_invest_firm_area where invest_firm_name = %s and area= %s'
    sql = 'insert into tb_invest_firm_area (invest_firm_name,area) values(%s,%s)'
    for value in area:
        if not cursor.execute(judge_sql, [base_info[0], value]):
            cursor.execute(sql, [base_info[0], value])
            print '[INFO]', time.ctime(), 'Executed', sql % tuple([base_info[0], value])
    judge_sql = 'select * from tb_investment_event where invest_firm_name = %s and invest_time = %s and project_name = %s and round = %s and amount = %s and area = %s and investor_name = %s'
    sql = 'insert into tb_investment_event (invest_firm_name,invest_time,project_name,round,amount,area,investor_name) VALUES (%s,%s,%s,%s,%s,%s,%s)'
    for event in events:
        value = [base_info[0]]
        value.extend(event[:-1])
        if not cursor.execute(judge_sql, value):
            cursor.execute(sql, value)
            print '[INFO]', time.ctime(), 'Executed', sql % tuple(value)
    judge_sql = 'select * from tb_event_link where event_name = %s  and event_link = %s'
    sql = 'insert into tb_event_link (event_name,event_link) VALUES (%s,%s)'
    for value in events_link:
        if not cursor.execute(judge_sql, value):
            cursor.execute(sql, value)
            print '[INFO]', time.ctime(), 'Executed', sql % value
    judge_sql = 'select * from tb_investor_link where investor_name = %s and investor_link= %s'
    sql = 'insert into tb_investor_link (investor_name,investor_link) VALUES (%s,%s)'
    for value in investors_link:
        if not cursor.execute(judge_sql, value):
            cursor.execute(sql, value)
            print '[INFO]', time.ctime(), 'Executed', sql % tuple(value)
    update_sql = 'update tb_invest_firm_link set update_time = %s where invest_firm_name = %s'
    cursor.execute(update_sql, [CURRENT_DATE, base_info[0]])
    print '[INFO]', time.ctime(), 'Executed', update_sql % tuple([CURRENT_DATE, base_info[0]])
    cursor.close()


def invest_firm_control():
    """
    This function is using for controlling the processing for the invest firm info fetching.
    """

    cursor = CONN.cursor()
    sql = 'select * from tb_invest_firm_link'
    res = cursor.execute(sql)
    res = cursor.fetchmany(res)
    for record in res:
        print '[INFO]', time.ctime(), 'Dealing', record[1], record[2], record[3]
        if record[3] == None:
            get_invest_firm_info(record[1], record[2])
        else:
            curr_time = time.strptime(CURRENT_DATE, '%Y%m%d')
            curr_time = datetime.datetime(curr_time[0], curr_time[1], curr_time[2])
            last_update_time = time.strptime(record[3], '%Y%m%d')
            last_update_time = datetime.datetime(last_update_time[0], last_update_time[1], last_update_time[2])
            delta_time = (curr_time - last_update_time).days
            if delta_time >= UPDATE_GAP:
                get_invest_firm_info(record[1], record[2])
    cursor.close()


def get_project_info(url):
    """
    This function is using for fetching, extracting and storing the project info.

    :param url: String
        The link of the project.
    """
    src = _fetch(url)
    base_info = []
    tags = []
    manager = []
    regex = '<div class="public-info pull-left">(.*?)</a>'
    res = re.findall(regex, src)
    if res.__len__() > 0:
        regex = '(.*?)/'
        res = _filter_html_tag(res[0])
        filter_res = re.findall(regex, res)
        if filter_res.__len__() > 0:
            base_info.append(_filter_html_tag(filter_res[0]))
        else:
            base_info.append(_filter_html_tag(res))
    else:
        base_info.append('')
    regex = '<ul class="detail-info">(.*?)</ul>'
    res = re.findall(regex, src)
    if res.__len__() > 0:
        res = res[0]
        regex = '<li>网址:  (.*?)</li>'
        link = re.findall(regex, res)
        if link.__len__() > 0:
            base_info.append(_filter_html_tag(link[0]))
        else:
            base_info.append('')
        regex = '<li>公司:  (.*?)</li>'
        company_name = re.findall(regex, res)
        if company_name.__len__() > 0:
            base_info.append(_filter_html_tag(company_name[0]))
        else:
            base_info.append('')
        regex = '<li>时间:  (.*?)</li>'
        company_time = re.findall(regex, res)
        if company_time.__len__() > 0:
            base_info.append(_filter_html_tag(company_time[0]))
        else:
            base_info.append('')
        regex = '<li>地点:  (.*?)</li>'
        location = re.findall(regex, res)
        if location.__len__() > 0:
            base_info.append(_filter_html_tag(location[0]))
        else:
            base_info.append('')
        regex = '<li>状态:  (.*?)</li>'
        status = re.findall(regex, res)
        if status.__len__() > 0:
            base_info.append(_filter_html_tag(status[0]))
        else:
            base_info.append('')
        regex = '<li>阶段:  (.*?)</li>'
        stage = re.findall(regex, res)
        if stage.__len__() > 0:
            base_info.append(_filter_html_tag(stage[0]))
        else:
            base_info.append('')
        regex = '<li>行业:(.*?)</li>'
        area = re.findall(regex, res)
        if area.__len__() > 0:
            base_info.append(_filter_html_tag(area[0]))
        else:
            base_info.append('')
        regex = '<li>子行业:(.*?)</li>'
        sub_area = re.findall(regex, res)
        if sub_area.__len__() > 0:
            base_info.append(_filter_html_tag(sub_area[0]))
        else:
            base_info.append('')
        regex = '<li>TAG:  (.*?)</li>'
        tag = re.findall(regex, res)
        if tag.__len__() > 0:
            tag = tag[0]
            tag = _filter_html_tag(tag)
            tag = tag.split(',')
            tags = map(lambda x: x.strip(), tag)
        regex = '<li>简介:  (.*?)</li>'
        intro = re.findall(regex, res)
        if intro.__len__() > 0:
            base_info.append(_filter_html_tag(intro[0]))
        else:
            base_info.append('')
    regex = '<a href="http://itjuzi.com/person/.*?</a>'
    res = re.findall(regex, src)
    tmp = []
    if res.__len__() > 0:
        for i in range(res.__len__()):
            if i % 2 == 0: continue
            tmp.append(res[i])
    if tmp.__len__() > 0:
        regex = 'href="(.*?)"'
        for record in tmp:
            link = re.findall(regex, record)[0]
            name = _filter_html_tag(record)
            manager.append([name, link])
    cursor = CONN.cursor()
    judge_sql = 'select * from tb_project_info where project_name = %s'
    if cursor.execute(judge_sql, [base_info[0]]):
        sql = 'update tb_project_info SET official_link = %s, company_name = %s, registration_time = %s, location = %s, project_status = %s, project_stage = %s, area = %s, sub_area = %s, intro = %s where project_name = %s'
        tmp_info = base_info[1:]
        tmp_info.extend([base_info[0]])
        cursor.execute(sql, tmp_info)
        print '[INFO]', time.ctime(), 'Executed', sql % tuple(tmp_info)
    else:
        sql = 'insert into tb_project_info (project_name, official_link, company_name, registration_time, location, project_status, project_stage, area, sub_area, intro) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        cursor.execute(sql, base_info)
        print '[INFO]', time.ctime(), 'Executed', sql % tuple(base_info)
    judge_sql = 'select * from tb_project_tag where project_name = %s and tag = %s'
    sql = 'insert into tb_project_tag (project_name, tag) VALUES (%s, %s)'
    for tag in tags:
        if not cursor.execute(judge_sql, [base_info[0], tag]):
            cursor.execute(sql, [base_info[0], tag])
            print '[INFO]', time.ctime(), 'Executed', sql % tuple([base_info[0], tag])
    judge_sql = 'select * from tb_project_manager_link where manager_name = %s and manager_link = %s'
    sql = 'insert into tb_project_manager_link (manager_name,manager_link) VALUES (%s,%s)'
    for record in manager:
        if not cursor.execute(judge_sql, record):
            cursor.execute(sql, record)
            print '[INFO]', time.ctime(), 'Executed', sql % tuple(record)
    sql = 'update tb_event_link set update_time = %s where event_name = %s'
    cursor.execute(sql, [CURRENT_DATE, base_info[0]])
    print '[INFO]', time.ctime(), 'Executed', sql % tuple([CURRENT_DATE, base_info[0]])
    cursor.close()


def event_control():
    """
    This function is using for controlling the event info fetching processing.
    """
    cursor = CONN.cursor()
    sql = 'select * from tb_event_link'
    res = cursor.execute(sql)
    res = cursor.fetchmany(res)
    counter = 0
    for index in range(res.__len__()):
        record = res[index]
        try:
            if record[3] == None:
                print '[INFO]', time.ctime(), 'Dealing', record[1], record[2], record[3]
                get_project_info(record[2])
            else:
                curr_time = time.strptime(CURRENT_DATE, '%Y%m%d')
                curr_time = datetime.datetime(curr_time[0], curr_time[1], curr_time[2])
                last_update_time = time.strptime(record[3], '%Y%m%d')
                last_update_time = datetime.datetime(last_update_time[0], last_update_time[1], last_update_time[2])
                delta_time = (curr_time - last_update_time).days
                if delta_time >= UPDATE_GAP:
                    print '[INFO]', time.ctime(), 'Dealing', record[1], record[2], record[3]
                    get_project_info(record[2])
        except:
            counter += 1
            # _wait()
            if counter < 3:
                index -= 1
                continue
            else:
                counter = 0
                continue
        finally:
            CONN.commit()
    cursor.close()


def get_investor_info(url):
    """
    This function is using for fetching, extracting and storing the investor info.
    :param url: String
        The url of the investor
    """

    src = _fetch(url)
    base_info = []
    area = []
    regex = '<a href="">(.*?)</a>'
    name = re.findall(regex, src)
    base_info.append(_res_judge(name))
    regex = '<ul class="detail-info">(.*?)</ul>'
    res = re.findall(regex, src)
    if res.__len__() > 0:
        regex = '<li>职位:  .*?</li>'
        tmp = re.findall(regex, res[0])
        if tmp.__len__() > 0:
            regex = '<a.*?</a>'
            invest_firm_name = re.findall(regex, tmp[0])
            base_info.append(_res_judge(invest_firm_name))
            regex = '</a>(.*?)</li>'
            position = re.findall(regex, tmp[0])
            base_info.append(_res_judge(position))
        regex = '<li>微博:  (.*?)</a>'
        weibo = re.findall(regex, res[0])
        base_info.append(_res_judge(weibo))
        regex = '<li>介绍:  (.*?)</li>'
        intro = re.findall(regex, res[0])
        base_info.append(_res_judge(intro))
    if res.__len__() > 1:
        res = res[1]
        regex = '<li>地点:(.*?)</li>'
        location = re.findall(regex, res)
        base_info.append(_res_judge(location))
        regex = '<li>工作:(.*?)</li>'
        work_company = re.findall(regex, res)
        base_info.append(_res_judge(work_company))
        regex = '<li>教育:(.*?)</li>'
        education = re.findall(regex, res)
        base_info.append(_res_judge(education))
        regex = '<li>投资领域:(.*?)</li>'
        tmp = re.findall(regex, res)
        if tmp.__len__() > 0:
            area = _filter_html_tag(tmp[0]).split(',')
            area = map(lambda x: x.strip(), area)
    cursor = CONN.cursor()
    judge_sql = 'select * from tb_invest_firm_investor where invest_firm_name = %s and investor_name = %s'
    sql = 'insert into tb_invest_firm_investor (investor_name,invest_firm_name,investor_position,weibo,intro,location,work_company,education) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'
    if not cursor.execute(judge_sql, [base_info[1], base_info[0]]):
        cursor.execute(sql, base_info)
        print '[INFO]', time.ctime(), 'Executed', sql % tuple(base_info)
    judge_sql = 'select * from tb_investor_area where investor_name = %s and area = %s'
    sql = 'insert into tb_investor_area (investor_name, area) VALUES (%s,%s)'
    for record in area:
        if not cursor.execute(judge_sql, [base_info[0], record]):
            cursor.execute(sql, [base_info[0], record])
            print '[INFO]', time.ctime(), 'Executed', sql % tuple([base_info[0], record])
    update_sql = 'update tb_investor_link set update_time = %s WHERE investor_name = %s'
    cursor.execute(update_sql, [CURRENT_DATE, base_info[0]])
    print '[INFO]', time.ctime(), 'Executed', update_sql % tuple([CURRENT_DATE, base_info[0]])
    cursor.close()


def investor_control():
    """
    This function is using for controling the processing of dealing with the investor info fetching.
    """
    print 'test' * 100
    cursor = CONN.cursor()
    sql = 'select * from tb_investor_link'
    res = cursor.execute(sql)
    res = cursor.fetchmany(res)
    counter = 0
    for index in range(res.__len__()):
        record = res[index]
        try:
            if record[3] == None:
                print '[INFO]', time.ctime(), 'Dealing', record[1], record[2], record[3]
                get_investor_info(record[2])
            else:
                curr_time = time.strptime(CURRENT_DATE, '%Y%m%d')
                curr_time = datetime.datetime(curr_time[0], curr_time[1], curr_time[2])
                last_update_time = time.strptime(record[3], '%Y%m%d')
                last_update_time = datetime.datetime(last_update_time[0], last_update_time[1], last_update_time[2])
                delta_time = (curr_time - last_update_time).days
                if delta_time >= UPDATE_GAP:
                    print '[INFO]', time.ctime(), 'Dealing', record[1], record[2], record[3]
                    get_investor_info(record[2])
        except Exception, e:
            print e
            counter += 1
            # _wait()
            if counter < 3:
                index -= 1
                continue
            else:
                counter = 0
                continue
        finally:
            CONN.commit()
    cursor.close()


def get_manager_info(url):
    """
    This function is using for fetching, extracting and storing the manager info.

    :param url: Stirng
        The url of the manager.
    """

    src = _fetch(url)
    base_info = []
    role = []
    regex = '<a href="">(.*?)</a>'
    name = re.findall(regex, src)
    base_info.append(_res_judge(name))
    regex = '<ul class="detail-info">(.*?)</ul>'
    res = re.findall(regex, src)
    if res.__len__() > 0:
        regex = '<li>职位:.*?</li>'
        tmp = re.findall(regex, res[0])
        if tmp.__len__() > 0:
            regex = '<a.*?</a>'
            invest_firm_name = re.findall(regex, tmp[0])
            base_info.append(_res_judge(invest_firm_name))
            regex = '</a>(.*?)</li>'
            position = re.findall(regex, tmp[0])
            base_info.append(_res_judge(position))
        regex = '<li>微博:  (.*?)</a>'
        weibo = re.findall(regex, res[0])
        base_info.append(_res_judge(weibo))
        regex = '<li>介绍:  (.*?)</li>'
        intro = re.findall(regex, res[0])
        base_info.append(_res_judge(intro))
    if res.__len__() > 1:
        res = res[1]
        regex = '<li>地点:(.*?)</li>'
        location = re.findall(regex, res)
        base_info.append(_res_judge(location))
        regex = '<li>工作:(.*?)</li>'
        work_company = re.findall(regex, res)
        base_info.append(_res_judge(work_company))
        regex = '<li>教育:(.*?)</li>'
        education = re.findall(regex, res)
        base_info.append(_res_judge(education))
        regex = '<li>角色:(.*?)</li>'
        tmp = re.findall(regex, res)
        if tmp.__len__() > 0:
            role = _filter_html_tag(tmp[0]).split(',')
            role = map(lambda x: x.strip(), role)
    cursor = CONN.cursor()
    judge_sql = 'select * from tb_project_manager where project_name = %s and manager_name = %s'
    sql = 'insert into tb_project_manager (manager_name,project_name,manager_position,weibo,intro,location,work_company,education) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'
    if not cursor.execute(judge_sql, [base_info[1], base_info[0]]):
        cursor.execute(sql, base_info)
        print '[INFO]', time.ctime(), 'Executed', sql % tuple(base_info)
    judge_sql = 'select * from tb_project_manager_role where manager_name = %s and manager_role = %s'
    sql = 'insert into tb_project_manager_role (manager_name,manager_role) VALUES (%s,%s)'
    for record in role:
        if not cursor.execute(judge_sql, [base_info[0], record]):
            cursor.execute(sql, [base_info[0], record])
            print '[INFO]', time.ctime(), 'Executed', sql % tuple([base_info[0], record])
    update_sql = 'update tb_project_manager_link set update_time = %s where manager_name = %s'
    cursor.execute(update_sql, [CURRENT_DATE, base_info[0]])
    print '[INFO]', time.ctime(), 'Executed', update_sql % tuple([CURRENT_DATE, base_info[0]])


def manager_control():
    """
    This function is using for controlling the processing of dealing with the manager info fetching.
    """
    cursor = CONN.cursor()
    sql = 'select * from tb_project_manager_link'
    res = cursor.execute(sql)
    res = cursor.fetchmany(res)
    counter = 0
    for index in range(res.__len__()):
        record = res[index]
        try:
            print '[INFO]', time.ctime(), 'Dealing', record[1], record[2], record[3]
            if record[3] == None:
                get_manager_info(record[2])
            else:
                curr_time = time.strptime(CURRENT_DATE, '%Y%m%d')
                curr_time = datetime.datetime(curr_time[0], curr_time[1], curr_time[2])
                last_update_time = time.strptime(record[3], '%Y%m%d')
                last_update_time = datetime.datetime(last_update_time[0], last_update_time[1], last_update_time[2])
                delta_time = (curr_time - last_update_time).days
                if delta_time >= UPDATE_GAP:
                    get_manager_info(record[2])
        except Exception, e:
            print e
            counter += 1
            # _wait()
            if counter < 3:
                index -= 1
                continue
            else:
                counter = 0
                continue
        finally:
            CONN.commit()
    cursor.close()


def _delete_record():
    cursor = CONN.cursor()
    sql = 'delete from tb_investment_event'
    cursor.execute(sql)
    sql = 'delete from tb_invest_firm_link'
    cursor.execute(sql)
    sql = 'delete from tb_invest_firm_base_info'
    cursor.execute(sql)
    sql = 'delete from tb_invest_firm_area'
    cursor.execute(sql)
    sql = 'delete from tb_invest_firm_investor'
    cursor.execute(sql)
    sql = 'delete from tb_invest_firm_stage'
    cursor.execute(sql)
    sql = 'delete from tb_investor_area'
    cursor.execute(sql)
    sql = 'delete from tb_investor_link'
    cursor.execute(sql)
    sql = 'delete from tb_project_info'
    cursor.execute(sql)
    sql = 'delete from tb_project_manager'
    cursor.execute(sql)
    sql = 'delete from tb_project_manager_link'
    cursor.execute(sql)
    sql = 'delete from tb_project_manager_role'
    cursor.execute(sql)
    sql = 'delete from tb_project_tag'
    cursor.execute(sql)
    sql = 'delete from tb_event_link'
    cursor.execute(sql)
    cursor.close()


def copy_data(src_table, target_table):
    cursor = CONN.cursor()
    select_sql = 'select * from %s'
    delete_sql = 'delete from %s'
    cursor.execute(delete_sql, [target_table])
    for record in cursor.fetchmany(cursor.execute(select_sql, [src_table])):
        params = ','.join(['%s'] * record.__len__())
        insert_sql = 'insert into %s VALUES (' + params + ')'
        tmp = [target_table]
        tmp.extend(params)
        cursor.execute(insert_sql, tmp)
        print '[INFO]', time.ctime(), 'Executed', insert_sql % tmp
    cursor.close()
    CONN.commit()


def update_control():
    cursor = CONN.cursor()
    sql = 'show tables'
    tables = cursor.fetchmany(cursor.execute(sql))
    auto_tables = []
    update_table = []
    for record in tables:
        if not record[0].find('auto') == -1:
            auto_tables.append(record[0])
            update_table.append(record[0].replace('_auto',''))

    for index in range(auto_tables.__len__()):
        delete_sql = 'delete from '+auto_tables[index]
        update_sql = 'insert into '+auto_tables[index]+' select * from '+update_table[index]
        print update_sql
        cursor.execute(delete_sql)
        cursor.execute(update_sql)
    cursor.close()

if __name__ == '__main__':
    while True:
        _delete_record()
        _login()
        get_invest_firm_link()
        while True:
            try:
                invest_firm_control()
            except:
                continue
            finally:
                CONN.commit()
            break
        while True:
            try:
                event_control()
            except:
                continue
            finally:
                CONN.commit()
            break
        while True:
            try:
                investor_control()
            except:
                continue
            finally:
                CONN.commit()
            break
        while True:
            try:
                manager_control()
            except:
                continue
            finally:
                CONN.commit()
            break
        while True:
            try:
                print '[INFO]',time.ctime(),'Executing program update VC Rank.'
                build_rank()
            except Exception,e:
                print e
                continue
            finally:
                CONN.commit()
            break
        while True:
            try:
                print '[INFO]',time.ctime(),'Executing program update VC Relation.'
                build_relation()
            except:
                continue
            finally:
                CONN.commit()
            break
        try:
            print '[INFO]',time.ctime(),'Executing program update data to auto tables.'
            update_control()
        except:
            continue
        finally:
            CONN.commit()
        break
    CONN.close()

