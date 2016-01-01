# coding=utf-8
__author__ = 'BorYee'

import MySQLdb
import numpy
import time



# ########## GLOBAL PARAM ####### #
CONN = MySQLdb.connect(user='root', host='localhost', db='db_investment')
CURR_YEAR = 2015
FEATURE_NAME = []
# ########## GLOBAL PARAM ####### #

def rank(feature_table_name, score_table_name):
    cursor = CONN.cursor()
    invest_firms = []
    feature_matrix = []
    rank_value = []
    sql = 'select * from ' + feature_table_name
    for record in cursor.fetchmany(cursor.execute(sql)):
        invest_firms.append(record[0])
        feature_matrix.append(map(float, record[1:]))
    feature_matrix = numpy.array(feature_matrix)
    feature_matrix = feature_matrix.transpose()
    for index in range(feature_matrix.__len__()):
        _max = max(feature_matrix[index])
        _min = min(feature_matrix[index])
        for inner_index in range(feature_matrix[index].__len__()):
            if not _max == _min:
                feature_matrix[index][inner_index] = (feature_matrix[index][inner_index] - _min) / (_max - _min)
            else:
                feature_matrix[index][inner_index] = 0
    feature_matrix = feature_matrix.transpose()
    for index in range(feature_matrix.__len__()):
        rank_value.append((invest_firms[index], sum(feature_matrix[index])))
    rank_value = sorted(rank_value, key=lambda x: x[1])
    sql = 'delete from ' + score_table_name
    cursor.execute(sql)
    sql = 'insert into ' + score_table_name + ' (invest_firm_name,score)values(%s,%s)'
    for record in rank_value:
        cursor.execute(sql, [record[0], record[1]])
        print '[INFO]', time.ctime(), sql % (record[0], record[1])
    cursor.close()


def feature_selection(table_name, begin='2015.01', end='2015.12'):
    """
    This function is using for extracting the features for ranking the invest firm.
        Features:
            area_number
            stage_number
            round_number
            each_area_event_number
            each_round_event_number
            each_year_event_number
            years_len
            each_area_event_amount
            each_round_event_amount
            each_year_event_amount
            listing_number
            each_age_event_number
            each_next_round_event_number
    """

    features = {}
    cursor = CONN.cursor()
    sql = 'select invest_firm_name from tb_invest_firm_base_info'
    invest_firm_list = cursor.fetchmany(cursor.execute(sql))
    for record in _fill(invest_firm_list, feature_area_number(begin, end)):
        features[record[0]] = record[1]
    for record in _fill(invest_firm_list, feature_stage_number()):
        features[record[0]].extend(record[1])
    for record in _fill(invest_firm_list, feature_round_number(begin, end)):
        features[record[0]].extend(record[1])
    for record in _fill(invest_firm_list, features_each_area_event_number(begin, end)):
        features[record[0]].extend(record[1])
    for record in _fill(invest_firm_list, features_each_round_event_number(begin, end)):
        features[record[0]].extend(record[1])
    for record in _fill(invest_firm_list, features_each_year_event_number()):
        features[record[0]].extend(record[1])
    for record in _fill(invest_firm_list, feature_years_len()):
        features[record[0]].extend(record[1])
    for record in _fill(invest_firm_list, features_each_area_event_amount(begin, end)):
        features[record[0]].extend(record[1])
    for record in _fill(invest_firm_list, features_each_round_event_amount(begin, end)):
        features[record[0]].extend(record[1])
    for record in _fill(invest_firm_list, features_each_year_event_amount()):
        features[record[0]].extend(record[1])
    for record in _fill(invest_firm_list, feature_listing_number(begin, end)):
        features[record[0]].extend(record[1])
    for record in _fill(invest_firm_list, features_each_age_event_number(begin, end)):
        features[record[0]].extend(record[1])
    for record in _fill(invest_firm_list, features_each_next_round_event_number(begin, end)):
        features[record[0]].extend(record[1])
    sql = 'delete from ' + table_name
    cursor.execute(sql)
    sql = 'insert into ' + table_name + ' values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    for record in features:
        tmp = [record]
        tmp.extend(features[record])
        cursor.execute(sql, tmp)
        print '[INFO]', time.ctime(), sql % tuple(tmp)
    cursor.close()


def _fill(invest_firm_list, feature):
    """
    This function is using for filling the matrix of features.

    :param invest_firm_list: list
        The invest firm list.

    :param feature: list
        A single feature value.

    :return res: list
        The filled feature list or matrix.
    """
    tmp_dict = {}
    if type(feature[0][1]) is list:
        tmp_list = [0 for i in range(list(feature[0][1]).__len__())]
    else:
        tmp_list = [0]
    for record in invest_firm_list:
        tmp_dict[record[0]] = tmp_list
    for record in feature:
        if record[0] in tmp_dict:
            if type(feature[0][1]) is list:
                tmp_dict[record[0]] = record[1]
            else:
                tmp_dict[record[0]] = [record[1]]
    res = []
    for record in tmp_dict:
        res.append((record, tmp_dict[record][:]))
    return res


def feature_area_number(begin, end):
    cursor = CONN.cursor()
    sql = 'SELECT invest_firm_name,COUNT(distinct(area)) FROM db_investment.tb_investment_event where invest_time > %s and invest_time < %s group by invest_firm_name'
    round_number = cursor.fetchmany(cursor.execute(sql, [begin, end]))
    FEATURE_NAME.append('领域数量')
    cursor.close()
    return round_number


def feature_stage_number():
    cursor = CONN.cursor()
    sql = 'SELECT invest_firm_name,count(distinct(stage)) FROM db_investment.tb_invest_firm_stage group by invest_firm_name'
    stage_number = cursor.fetchmany(cursor.execute(sql))
    FEATURE_NAME.append('阶段数量')
    cursor.close()
    return stage_number


def feature_round_number(begin, end):
    cursor = CONN.cursor()
    sql = 'SELECT invest_firm_name,COUNT(distinct(round)) FROM db_investment.tb_investment_event where invest_time > %s and invest_time < %s group by invest_firm_name'
    round_number = cursor.fetchmany(cursor.execute(sql, [begin, end]))
    FEATURE_NAME.append('轮次数量')
    cursor.close()
    return round_number


def features_each_area_event_number(begin, end):
    cursor = CONN.cursor()
    each_area_event_number = []
    area_index = {}
    event_dict = {}
    sql = 'SELECT DISTINCT(area) FROM db_investment.tb_investment_event'
    area_list = cursor.fetchmany(cursor.execute(sql))
    for index in range(area_list.__len__()):
        FEATURE_NAME.append(area_list[index][0] + '数量')
        area_index[area_list[index][0]] = index
    sql = 'SELECT invest_firm_name, area, COUNT(1) FROM db_investment.tb_investment_event where invest_time > %s and invest_time < %s GROUP BY invest_firm_name,area'
    for record in cursor.fetchmany(cursor.execute(sql, [begin, end])):
        if record[0] not in event_dict:
            event_dict[record[0]] = [(record[1], record[2])]
        else:
            event_dict[record[0]].append((record[1], record[2]))
    for record in event_dict:
        tmp = [0 for i in range(area_list.__len__())]
        for area_event_number in event_dict[record]:
            tmp[area_index[area_event_number[0]]] = area_event_number[1]
        each_area_event_number.append((record, tmp))
    cursor.close()
    return each_area_event_number


def features_each_round_event_number(begin, end):
    cursor = CONN.cursor()
    each_round_event_number = []
    round_index = {}
    event_dict = {}
    sql = 'SELECT DISTINCT(round) FROM db_investment.tb_investment_event'
    round_list = cursor.fetchmany(cursor.execute(sql))
    for index in range(round_list.__len__()):
        FEATURE_NAME.append(round_list[index][0] + '数量')
        round_index[round_list[index][0]] = index
    sql = 'SELECT invest_firm_name,round,count(1) FROM db_investment.tb_investment_event where invest_time > %s and invest_time < %s group by invest_firm_name,round'
    for record in cursor.fetchmany(cursor.execute(sql, [begin, end])):
        if record[0] not in event_dict:
            event_dict[record[0]] = [(record[1], record[2])]
        else:
            event_dict[record[0]].append((record[1], record[2]))
    for record in event_dict:
        tmp = [0 for i in range(round_list.__len__())]
        for round_event_number in event_dict[record]:
            tmp[round_index[round_event_number[0]]] = round_event_number[1]
        each_round_event_number.append((record, tmp))
    cursor.close()
    return each_round_event_number


def features_each_year_event_number():
    cursor = CONN.cursor()
    each_year_event_number = []
    year_index = {}
    event_dict = {}
    sql = 'SELECT distinct(invest_time) FROM db_investment.tb_investment_event'
    year_list = cursor.fetchmany(cursor.execute(sql))
    year_list = map(lambda x: x[0].split('.')[0], year_list)
    year_list = [i for i in set(year_list)]
    for index in range(year_list.__len__()):
        FEATURE_NAME.append(year_list[index] + '数量')
        year_index[year_list[index]] = index
    sql = 'SELECT invest_firm_name,count(1) FROM db_investment.tb_investment_event where invest_time like %s group by invest_firm_name'
    for year in year_list:
        for record in cursor.fetchmany(cursor.execute(sql, ['%' + year + '%'])):
            if record[0] not in event_dict:
                event_dict[record[0]] = [(year, record[1])]
            else:
                event_dict[record[0]].append((year, record[1]))
    for record in event_dict:
        tmp = [0 for i in range(year_list.__len__())]
        for year_event_number in event_dict[record]:
            tmp[year_index[year_event_number[0]]] = year_event_number[1]
        each_year_event_number.append((record, tmp))
    cursor.close()
    return each_year_event_number


def feature_years_len():
    cursor = CONN.cursor()
    res = []
    sql = 'SELECT invest_firm_name,min(invest_time) FROM db_investment.tb_investment_event group by invest_firm_name'
    years = cursor.fetchmany(cursor.execute(sql))
    for record in years:
        year_len = 2015 - int(record[1].split('.')[0])
        res.append((record[0], year_len))
    FEATURE_NAME.append('机构年龄')
    return res


def features_each_area_event_amount(begin, end):
    cursor = CONN.cursor()
    each_area_event_amount = []
    area_index = {}
    event_dict = {}
    sql = 'select distinct(area) from tb_amount_tran'
    area_list = cursor.fetchmany(cursor.execute(sql))
    for index in range(area_list.__len__()):
        FEATURE_NAME.append(area_list[index][0] + '金额')
        area_index[area_list[index][0]] = index
    sql = 'SELECT invest_firm_name,area,sum(amount) FROM db_investment.tb_amount_tran where invest_time > %s and invest_time < %s group by invest_firm_name,area'
    for record in cursor.fetchmany(cursor.execute(sql, [begin, end])):
        if record[0] not in event_dict:
            event_dict[record[0]] = [(record[1], record[2])]
        else:
            event_dict[record[0]].append((record[1], record[2]))
    for record in event_dict:
        tmp = [0 for i in range(area_list.__len__())]
        for area_amount in event_dict[record]:
            tmp[area_index[area_amount[0]]] = area_amount[1]
        each_area_event_amount.append((record, tmp))
    cursor.close()
    return each_area_event_amount


def features_each_round_event_amount(begin, end):
    cursor = CONN.cursor()
    each_round_event_amount = []
    round_index = {}
    event_dict = {}
    sql = 'select distinct(round) from tb_amount_tran'
    round_list = cursor.fetchmany(cursor.execute(sql))
    for index in range(round_list.__len__()):
        FEATURE_NAME.append(round_list[index][0] + '金额')
        round_index[round_list[index][0]] = index
    sql = 'SELECT invest_firm_name,round,sum(amount) FROM db_investment.tb_amount_tran where invest_time > %s and invest_time < %s group by invest_firm_name,round'
    for record in cursor.fetchmany(cursor.execute(sql, [begin, end])):
        if record[0] not in event_dict:
            event_dict[record[0]] = [(record[1], record[2])]
        else:
            event_dict[record[0]].append((record[1], record[2]))
    for record in event_dict:
        tmp = [0 for i in range(round_list.__len__())]
        for area_amount in event_dict[record]:
            tmp[round_index[area_amount[0]]] = area_amount[1]
        each_round_event_amount.append((record, tmp))
    cursor.close()
    return each_round_event_amount


def features_each_year_event_amount():
    cursor = CONN.cursor()
    each_year_event_amount = []
    year_index = {}
    event_dict = {}
    sql = 'SELECT distinct(invest_time) FROM db_investment.tb_investment_event'
    year_list = cursor.fetchmany(cursor.execute(sql))
    year_list = map(lambda x: x[0].split('.')[0], year_list)
    year_list = [i for i in set(year_list)]
    for index in range(year_list.__len__()):
        FEATURE_NAME.append(year_list[index] + '金额')
        year_index[year_list[index]] = index
    sql = 'SELECT invest_firm_name,sum(amount) FROM db_investment.tb_amount_tran where invest_time like %s group by invest_firm_name'
    for year in year_list:
        for record in cursor.fetchmany(cursor.execute(sql, ['%' + year + '%'])):
            if record[0] not in event_dict:
                event_dict[record[0]] = [(year, record[1])]
            else:
                event_dict[record[0]].append((year, record[1]))
    for record in event_dict:
        tmp = [0 for i in range(year_list.__len__())]
        for year_event_amount in event_dict[record]:
            tmp[year_index[year_event_amount[0]]] = year_event_amount[1]
        each_year_event_amount.append((record, tmp))
    cursor.close()
    return each_year_event_amount


def tran_amount():
    exchange = {
        '美元': 6.3471,
        '日元': 0.0531,
        '人民币': 1,
        '欧元': 7.1989,
        '新台币': 0.1962
    }
    fuzzy_amount = {
        '数十万': 300000,
        '数百万': 3000000,
        '数千万': 30000000,
        '亿元及以上': 300000000,
        '未透露': 3000000
    }
    dimension = {
        '万': 10000,
        '亿': 100000000
    }
    cursor = CONN.cursor()
    sql = 'SELECT project_name,invest_time,count(1) FROM db_investment.tb_investment_event group by project_name,invest_time'
    res = cursor.fetchmany(cursor.execute(sql))
    co_investment = {}
    for record in res:
        if int(record[2]) > 1:
            co_investment[(record[0], record[1])] = int(record[2])
    sql = 'delete from tb_amount_tran'
    cursor.execute(sql)
    sql = 'SELECT * FROM db_investment.tb_investment_event'
    insert_sql = 'insert into tb_amount_tran (invest_firm_name,project_name,invest_time,round,amount,area,investor_name) VALUES (%s,%s,%s,%s,%s,%s,%s)'
    res = cursor.fetchmany(cursor.execute(sql))
    for record in res:
        amount = 0
        for key in fuzzy_amount:
            if key in record[5]:
                amount = fuzzy_amount[key]
                break
        if not amount:
            for key in dimension:
                if key in record[5]:
                    amount = float(record[5][0:record[5].index(key)])
                    amount *= dimension[key]
                    break
        for key in exchange:
            if key in record[5]:
                amount *= exchange[key]
                break
        if (record[2], record[3]) in co_investment:
            amount /= float(co_investment[(record[2], record[3])])
        cursor.execute(insert_sql, [record[1], record[2], record[3], record[4], amount, record[6], record[7]])
    cursor.close()


def feature_listing_number(begin, end):
    cursor = CONN.cursor()
    sql = 'SELECT tb_investment_event.invest_firm_name,count(1) FROM db_investment.tb_project_info,tb_investment_event where tb_investment_event.project_name=tb_project_info.project_name and project_stage = \'上市公司\' group by tb_investment_event.invest_firm_name and invest_time > %s and invest_time < %s'
    listing_number = cursor.fetchmany(cursor.execute(sql, [begin, end]))
    cursor.close()
    FEATURE_NAME.append('上市数量')
    return listing_number


def features_each_age_event_number(begin, end):
    cursor = CONN.cursor()
    sql = 'select tb_investment_event.invest_firm_name,tb_project_info.project_name,tb_project_info.registration_time from tb_investment_event,tb_project_info where tb_investment_event.project_name = tb_project_info.project_name and project_status <> \'已关闭\' and tb_investment_event.invest_time > %s and tb_investment_event.invest_time < %s group by tb_investment_event.invest_firm_name,tb_project_info.project_name'
    registration_list = cursor.fetchmany(cursor.execute(sql, [begin, end]))
    each_age_event_number = []
    event_dict = {}
    for record in registration_list:
        if record[0] not in event_dict:
            event_dict[record[0]] = [(record[1], record[2])]
        else:
            event_dict[record[0]].append((record[1], record[2]))
    for key in event_dict:
        tmp_list = [ele for ele in set(event_dict[key])]
        tmp = [0 for i in range(6)]
        for record in tmp_list:
            year = int(record[1][0:record[1].index('年')])
            delta = CURR_YEAR - year
            if delta >= 5:
                tmp[5] += 1
            elif delta < 0:
                tmp[0] += 1
            else:
                tmp[delta] += 1
        each_age_event_number.append((key, tmp[:]))
    cursor.close()
    for i in range(6):
        FEATURE_NAME.append('所投企业年龄' + str(i))
    return each_age_event_number


def features_each_next_round_event_number(begin, end):
    cursor = CONN.cursor()
    sql = 'SELECT invest_firm_name,project_name,invest_time FROM db_investment.tb_investment_event where invest_time > %s and invest_time < %s order by invest_time asc'
    event_dict = {}
    event_invest_dict = {}
    each_next_round_event_number = []
    res = cursor.fetchmany(cursor.execute(sql, [begin, end]))
    for record in res:
        if record[0] not in event_dict:
            event_dict[record[0]] = [(record[1], record[2])]
        else:
            event_dict[record[0]].append((record[1], record[2]))
        if record[1] not in event_invest_dict:
            event_invest_dict[record[1]] = [record[2]]
        else:
            event_invest_dict[record[1]].append(record[2])
    for key in event_invest_dict:
        event_invest_dict[key] = [ele for ele in set(event_invest_dict[key])]
    for key in event_dict:
        tmp = [0 for i in range(6)]
        for record in event_dict[key]:
            round_increment = event_invest_dict[record[0]].__len__() - event_invest_dict[record[0]].index(record[1]) - 1
            if round_increment > 5:
                tmp[5] += 1
            elif round_increment < 0:
                tmp[0] += 1
            else:
                tmp[round_increment] += 1
        each_next_round_event_number.append((key, tmp[:]))
    for i in range(6):
        FEATURE_NAME.append('所投企业后续获得投资轮数' + str(i))
    cursor.close()
    return each_next_round_event_number


def feature_table_judge(table_name, db_name):
    cursor = CONN.cursor()
    sql = 'SELECT count(*) FROM information_schema.TABLES WHERE table_name = %s AND TABLE_SCHEMA = %s'
    res = cursor.fetchmany(cursor.execute(sql, [table_name, 'db_investment']))
    if res[0][0] == 0:
        sql = 'CREATE TABLE ' + table_name + ' (  `机构名称` text COLLATE utf8_unicode_ci,  `领域数量` text COLLATE utf8_unicode_ci,  `阶段数量` text COLLATE utf8_unicode_ci,  `轮次数量` text COLLATE utf8_unicode_ci,  `金融数量` text COLLATE utf8_unicode_ci,  `旅游数量` text COLLATE utf8_unicode_ci,  `硬件数量` text COLLATE utf8_unicode_ci,  `汽车交通数量` text COLLATE utf8_unicode_ci,  `企业服务数量` text COLLATE utf8_unicode_ci,  `本地生活数量` text COLLATE utf8_unicode_ci,  `电子商务数量` text COLLATE utf8_unicode_ci,  `教育数量` text COLLATE utf8_unicode_ci,  `广告营销数量` text COLLATE utf8_unicode_ci,  `游戏数量` text COLLATE utf8_unicode_ci,  `文化娱乐体育数量` text COLLATE utf8_unicode_ci,  `医疗健康数量` text COLLATE utf8_unicode_ci,  `房产服务数量` text COLLATE utf8_unicode_ci,  `SNS社交网络数量` text COLLATE utf8_unicode_ci,  `移动互联网数量` text COLLATE utf8_unicode_ci,  `工具软件数量` text COLLATE utf8_unicode_ci,  `未知领域数量` text COLLATE utf8_unicode_ci,  `A轮数量` text COLLATE utf8_unicode_ci,  `收购数量` text COLLATE utf8_unicode_ci,  `种子天使数量` text COLLATE utf8_unicode_ci,  `Pre_A轮数量` text COLLATE utf8_unicode_ci,  `B轮数量` text COLLATE utf8_unicode_ci,  `C轮数量` text COLLATE utf8_unicode_ci,  `战略投资数量` text COLLATE utf8_unicode_ci,  `D轮数量` text COLLATE utf8_unicode_ci,  `F轮_PreIPO数量` text COLLATE utf8_unicode_ci,  `E轮数量` text COLLATE utf8_unicode_ci,  `不明确数量` text COLLATE utf8_unicode_ci,  `IPO上市及以后数量` text COLLATE utf8_unicode_ci,  `1999数量` text COLLATE utf8_unicode_ci,  `2002数量` text COLLATE utf8_unicode_ci,  `2003数量` text COLLATE utf8_unicode_ci,  `2000数量` text COLLATE utf8_unicode_ci,  `2006数量` text COLLATE utf8_unicode_ci,  `2007数量` text COLLATE utf8_unicode_ci,  `2004数量` text COLLATE utf8_unicode_ci,  `2005数量` text COLLATE utf8_unicode_ci,  `2015数量` text COLLATE utf8_unicode_ci,  `2014数量` text COLLATE utf8_unicode_ci,  `2008数量` text COLLATE utf8_unicode_ci,  `2009数量` text COLLATE utf8_unicode_ci,  `2011数量` text COLLATE utf8_unicode_ci,  `2010数量` text COLLATE utf8_unicode_ci,  `2013数量` text COLLATE utf8_unicode_ci,  `2012数量` text COLLATE utf8_unicode_ci,  `机构年龄` text COLLATE utf8_unicode_ci,  `金融金额` text COLLATE utf8_unicode_ci,  `旅游金额` text COLLATE utf8_unicode_ci,  `硬件金额` text COLLATE utf8_unicode_ci,  `汽车交通金额` text COLLATE utf8_unicode_ci,  `企业服务金额` text COLLATE utf8_unicode_ci,  `本地生活金额` text COLLATE utf8_unicode_ci,  `电子商务金额` text COLLATE utf8_unicode_ci,  `教育金额` text COLLATE utf8_unicode_ci,  `广告营销金额` text COLLATE utf8_unicode_ci,  `游戏金额` text COLLATE utf8_unicode_ci,  `文化娱乐体育金额` text COLLATE utf8_unicode_ci,  `医疗健康金额` text COLLATE utf8_unicode_ci,  `房产服务金额` text COLLATE utf8_unicode_ci,  `SNS社交网络金额` text COLLATE utf8_unicode_ci,  `移动互联网金额` text COLLATE utf8_unicode_ci,  `工具软件金额` text COLLATE utf8_unicode_ci,  `未知领域金额` text COLLATE utf8_unicode_ci,  `A轮金额` text COLLATE utf8_unicode_ci,  `收购金额` text COLLATE utf8_unicode_ci,  `种子天使金额` text COLLATE utf8_unicode_ci,  `Pre_A轮金额` text COLLATE utf8_unicode_ci,  `B轮金额` text COLLATE utf8_unicode_ci,  `C轮金额` text COLLATE utf8_unicode_ci,  `战略投资金额` text COLLATE utf8_unicode_ci,  `D轮金额` text COLLATE utf8_unicode_ci,  `F轮_PreIPO金额` text COLLATE utf8_unicode_ci,  `E轮金额` text COLLATE utf8_unicode_ci,  `不明确金额` text COLLATE utf8_unicode_ci,  `IPO上市及以后金额` text COLLATE utf8_unicode_ci,  `1999金额` text COLLATE utf8_unicode_ci,  `2002金额` text COLLATE utf8_unicode_ci,  `2003金额` text COLLATE utf8_unicode_ci,  `2000金额` text COLLATE utf8_unicode_ci,  `2006金额` text COLLATE utf8_unicode_ci,  `2007金额` text COLLATE utf8_unicode_ci,  `2004金额` text COLLATE utf8_unicode_ci,  `2005金额` text COLLATE utf8_unicode_ci,  `2015金额` text COLLATE utf8_unicode_ci,  `2014金额` text COLLATE utf8_unicode_ci,  `2008金额` text COLLATE utf8_unicode_ci,  `2009金额` text COLLATE utf8_unicode_ci,  `2011金额` text COLLATE utf8_unicode_ci,  `2010金额` text COLLATE utf8_unicode_ci,  `2013金额` text COLLATE utf8_unicode_ci,  `2012金额` text COLLATE utf8_unicode_ci,  `上市数量` text COLLATE utf8_unicode_ci,  `所投企业年龄0` text COLLATE utf8_unicode_ci,  `所投企业年龄1` text COLLATE utf8_unicode_ci,  `所投企业年龄2` text COLLATE utf8_unicode_ci,  `所投企业年龄3` text COLLATE utf8_unicode_ci,  `所投企业年龄4` text COLLATE utf8_unicode_ci,  `所投企业年龄5` text COLLATE utf8_unicode_ci,  `所投企业后续获得投资轮数0` text COLLATE utf8_unicode_ci,  `所投企业后续获得投资轮数1` text COLLATE utf8_unicode_ci,  `所投企业后续获得投资轮数2` text COLLATE utf8_unicode_ci,  `所投企业后续获得投资轮数3` text COLLATE utf8_unicode_ci,  `所投企业后续获得投资轮数4` text COLLATE utf8_unicode_ci,  `所投企业后续获得投资轮数5` text COLLATE utf8_unicode_ci) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci'
        cursor.execute(sql)
    cursor.close()


def score_table_judge(table_name, db_name):
    cursor = CONN.cursor()
    sql = 'SELECT count(*) FROM information_schema.TABLES WHERE table_name = %s AND TABLE_SCHEMA = %s'
    res = cursor.fetchmany(cursor.execute(sql, [table_name, 'db_investment']))
    if res[0][0] == 0:
        sql = 'CREATE TABLE ' + table_name + ' (  `id` int(11) NOT NULL AUTO_INCREMENT,  `invest_firm_name` text COLLATE utf8_unicode_ci,  `score` double DEFAULT NULL,  PRIMARY KEY (`id`)) ENGINE=InnoDB AUTO_INCREMENT=4111 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci'
        cursor.execute(sql)
    cursor.close()


def build_rank(feature_table_name='tb_invest_firm_features_201501_201512',
               score_table_name='tb_invest_firm_score_201501_201512', begin='2015.01', end='2015.12'):
    feature_table_judge(feature_table_name, 'db_investment')
    score_table_judge(score_table_name, 'db_investment')
    tran_amount()
    feature_selection(feature_table_name, begin, end)
    rank(feature_table_name, score_table_name)
    CONN.commit()
    CONN.close()

