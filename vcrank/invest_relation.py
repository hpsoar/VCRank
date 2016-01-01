# coding=utf-8
__author__ = 'BorYee'

import MySQLdb
import time

# ########## GLOBAL PARAM ####### #
CONN = MySQLdb.connect(host='localhost', user='root', db='db_investment')
# ########## GLOBAL PARAM ####### #


def build_network_matrix():
    event_dict = {}
    invest_firm_list = []
    invest_firm_dict = {}
    cursor = CONN.cursor()
    sql = 'SELECT invest_firm_name,project_name FROM db_investment.tb_investment_event'
    res = cursor.fetchmany(cursor.execute(sql))
    for record in res:
        if record[1] in event_dict:
            event_dict[record[1]].append(record[0])
        else:
            event_dict[record[1]] = [record[0]]
    for record in event_dict:
        if event_dict[record].__len__() > 1:
            invest_firm_list.extend(event_dict[record])
    for index in range(invest_firm_list.__len__()):
        invest_firm_dict[invest_firm_list[index]] = index
    network = [[0 for i in range(invest_firm_list.__len__())] for j in range(invest_firm_list.__len__())]
    for project in event_dict:
        if event_dict[project].__len__() > 1:
            for index_i in range(event_dict[project].__len__()):
                for index_j in range(event_dict[project].__len__()):
                    if index_i != index_j:
                        network[invest_firm_dict[event_dict[project][index_i]]][
                            invest_firm_dict[event_dict[project][index_i]]] += 1
    cursor.close()
    return invest_firm_list, network


def build_network_gephi():
    event_dict = {}
    invest_firm_list = []
    invest_firm_dict = {}
    relation = {}
    network = []
    cursor = CONN.cursor()
    sql = 'SELECT invest_firm_name,project_name FROM db_investment.tb_investment_event'
    res = cursor.fetchmany(cursor.execute(sql))
    for record in res:
        if record[1] in event_dict:
            event_dict[record[1]].append(record[0])
        else:
            event_dict[record[1]] = [record[0]]
    for record in event_dict:
        event_dict[record] = [ele for ele in set(event_dict[record])]
        if event_dict[record].__len__() > 1:
            invest_firm_list.extend(event_dict[record])
    invest_firm_list = [ele for ele in set(invest_firm_list)]
    for index in range(invest_firm_list.__len__()):
        invest_firm_dict[invest_firm_list[index]] = index
    for project in event_dict:
        if event_dict[project].__len__() > 1:
            for index_i in range(event_dict[project].__len__() - 1):
                for index_j in range(index_i + 1, event_dict[project].__len__()):
                    if (event_dict[project][index_i], event_dict[project][index_j]) in relation:
                        relation[(event_dict[project][index_i], event_dict[project][index_j])] += 1
                        continue
                    elif (event_dict[project][index_j], event_dict[project][index_i]) in relation:
                        relation[(event_dict[project][index_j], event_dict[project][index_i])] += 1
                        continue
                    else:
                        relation[(event_dict[project][index_i], event_dict[project][index_j])] = 1
    sql = 'delete from tb_relation'
    cursor.execute(sql)
    sql = 'insert into tb_relation (source_node,target_node) VALUES (%s,%s)'
    for record in relation:
        cursor.execute(sql, [record[0], record[1]])
        network.append((invest_firm_dict[record[0]], invest_firm_dict[record[1]]))
    cursor.close()
    return invest_firm_list, network


def load_detection_res():
    cursor = CONN.cursor()
    sql = 'delete from tb_community'
    cursor.execute(sql)
    sql = 'insert into tb_community (node,community) VALUES (%s,%s)'
    with open('../res/community_res.csv', 'r') as f:
        f.readline()
        for record in f.readlines():
            record = record.split(',')
            cursor.execute(sql, record)
    cursor.close()


def build_relation():
    investment = {}
    cursor = CONN.cursor()
    sql = 'delete from tb_detail_relation'
    cursor.execute(sql)
    sql = 'SELECT * FROM db_investment.tb_investment_event where round not like "%不明确%" order by project_name desc'
    res = cursor.fetchmany(cursor.execute(sql))
    for record in res:
        project_name = record[2]
        invest_firm = record[1]
        round = record[4]
        if project_name in investment:
            if round in investment[project_name]:
                investment[project_name][round].append(invest_firm)
            else:
                investment[project_name][round] = [invest_firm]
        else:
            investment[project_name] = {round: [invest_firm]}
    round_index = {
        0: '种子天使',
        1: 'Pre-A轮',
        2: 'A轮',
        3: 'B轮',
        4: 'C轮',
        5: 'D轮',
        6: 'E轮',
        7: 'F轮-PreIPO',
        8: 'IPO上市及以后',
        9: '收购'
    }
    for project in investment:
        relation = investment[project]
        re_list = []
        for index in range(10):
            if round_index[index] in relation:
                invest_firm_list = relation[round_index[index]]
                re_list.append((round_index[index], invest_firm_list))
                sql = 'insert into tb_detail_relation (source_firm,target_firm,relation_type,round) VALUES (%s,%s,%s,%s)'
                for if_index_out in range(invest_firm_list.__len__()):
                    for if_index_in in range(invest_firm_list.__len__()):
                        if not if_index_out == if_index_in:
                            print '[INFO]', time.ctime(), 'Executed ', sql % (
                                invest_firm_list[if_index_in], invest_firm_list[if_index_out], 'co', round_index[index])
                            cursor.execute(sql, [invest_firm_list[if_index_in], invest_firm_list[if_index_out], 'co',
                                                 round_index[index]])
                            print '[INFO]', time.ctime(), 'Executed ', sql % (
                                invest_firm_list[if_index_in], invest_firm_list[if_index_out], 'co', round_index[index])
        if re_list.__len__() > 1:
            for index in range(re_list.__len__() - 1):
                src_round, src_list = re_list[index]
                trg_round, trg_list = re_list[index + 1]
                sql = 'insert into tb_detail_relation (source_firm,target_firm,relation_type,round) VALUES (%s,%s,%s,%s)'
                for src_firm in src_list:
                    for trg_firm in trg_list:
                        cursor.execute(sql, [src_firm, trg_firm, 'of', trg_round])
                        print '[INFO]', time.ctime(), 'Executed ', sql % (src_firm, trg_firm, 'of', trg_round)
    cursor.close()
    CONN.commit()
    CONN.close()


if __name__ == '__main__':
    try:
        build_relation()
    finally:
        CONN.commit()
        CONN.close()