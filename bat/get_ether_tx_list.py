#!/usr/bin/env python3
#_*_ coding: utf-8 _*_

import requests
import configparser
from decimal import Decimal
import os
import datetime
import time
import sys
import socket
import pymysql


class SetEnv:
    def __init__(self):
        self.ip = socket.gethostbyname(socket.gethostname())
        self.abs = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        inifile = configparser.ConfigParser()
        inifile.read(self.abs + '/bat/config.ini', 'UTF-8-sig')

        if self.ip == inifile.get('IP', 'DEV_IP_ADDR'):
            self.env = 'develop'
        elif self.ip == inifile.get('IP', 'REAL_IP_ADDR'):
            self.env = 'product'
        else:
            self.env = 'local'


class SetPath:
    def __init__(self):
        self.set_env = SetEnv()
        self.env = self.set_env.env
        self.abs_path = self.set_env.abs

        if self.env == 'local':
            self.sql_path = 'LOCAL_SQL'
            self.etherscan_path = 'DEV_ETHERSCAN_API'
        elif self.env == 'develop':
            self.sql_path = 'DEV_SQL'
            self.etherscan_path = 'DEV_ETHERSCAN_API'
        elif self.env == 'product':
            self.sql_path = 'REAL_SQL'
            self.etherscan_path = 'ETHERSCAN_API'


class MysqlClass():
    def __init__(self, db, dbuser, dbpasswd, dbhost, dbcharset):
        self.db = db
        self.user = dbuser
        self.passwd = dbpasswd
        self.host = dbhost
        self.charset = dbcharset

    def db_connect(self):
        try:
            conn = pymysql.connect(
                db=self.db,
                user=self.user,
                passwd=self.passwd,
                host=self.host,
                charset=self.charset)
            cur = conn.cursor()
            return cur, conn
        except pymysql.Error as msg:
            print('MySQL Error: ', msg)
            sys.exit(0)


def main():
    alog(set_alog_nm(), "[INFO] main() - START")
    set_path = SetPath()
    sql_path = set_path.sql_path
    etherscan_api = set_path.etherscan_path

    inifile = configparser.ConfigParser()
    inifile.read(set_path.abs_path + '/bat/config.ini', 'UTF-8-sig')

    db = inifile.get(sql_path, 'DATABASE')
    dbuser = inifile.get(sql_path, 'USER')
    dbpasswd = inifile.get(sql_path, 'PASSWORD')
    dbhost = inifile.get(sql_path, 'HOST')
    dbcharset = inifile.get(sql_path, 'CHARSET')

    url = inifile.get(etherscan_api, 'URL')
    p_module = inifile.get(etherscan_api, 'MODULE')
    p_startblock = inifile.get(etherscan_api, 'START_BLOCK')
    p_endblock = inifile.get(etherscan_api, 'END_BLOCK')
    p_sort = inifile.get(etherscan_api, 'SORT')
    p_apikey = inifile.get(etherscan_api, 'API_KEY')
    p_address = inifile.get(etherscan_api, 'ADDRESS')

    token_tx = get_token_tx(url, p_module, p_startblock, p_endblock, p_sort, p_apikey, p_address)
    tx_list = get_tx_list(url, p_module, p_startblock, p_endblock, p_sort, p_apikey, p_address)
    startblock = tx_list[1]
    orders_list = create_tx_list(tx_list[0], token_tx)

    if not orders_list:
        update_start_block(startblock, etherscan_api, inifile, set_path.abs_path)
    else:
        result = mk_orders_list(db, dbuser, dbpasswd, dbhost, dbcharset, orders_list)
        if result is True:
            update_start_block(startblock, etherscan_api, inifile, set_path.abs_path)


def get_token_tx(url, p_module, p_startblock, p_endblock, p_sort, p_apikey, p_address):

    alog(set_alog_nm(), "[INFO] get_token_tx()")

    p_action = "tokentx"
    parms = (('module', p_module), ('action', p_action), ('address', p_address), ('startblock', p_startblock),
             ('endblock', p_endblock), ('sort', p_sort), ('apikey', p_apikey))
    res = requests.get(url=url, data=parms, timeout=1800)

    if res.status_code == 200:
        alog(set_alog_nm(), "[INFO] get_token_tx() - Http request successfully with status code:" + str(res.status_code))
    else:
        alog(set_alog_nm(), "[ERROR] get_token_tx() - Http request error occurred with status code:" + str(res.status_code))

    jbt = res.json()

    return jbt


def get_tx_list(url, p_module, p_startblock, p_endblock, p_sort, p_apikey, p_address):

    alog(set_alog_nm(), "[INFO] get_tx_list()")

    p_action = "txlist"
    parms = (('module', p_module), ('action', p_action), ('address', p_address), ('startblock', p_startblock),
             ('endblock', p_endblock), ('sort', p_sort), ('apikey', p_apikey))
    res = requests.get(url=url, data=parms, timeout=1800)

    if res.status_code == 200:
        alog(set_alog_nm(), "[INFO] get_tx_list() - Http request successfully with status code:" + str(res.status_code))
    else:
        alog(set_alog_nm(), "[ERROR] get_tx_list() - Http request error occurred with status code:" + str(res.status_code))

    jbt = res.json()

    if jbt["status"] == str(0):
        alog(set_alog_nm(), "[INFO] get_tx_list() - transaction list count 0 exit.")
        sys.exit(0)
    else:
        startblock = int(jbt['result'][0]['blockNumber']) + 1
        alog(set_alog_nm(), "[INFO] get_tx_list() - get start block:" + str(startblock))
    return jbt, startblock


def create_tx_list(tx_list, token_tx):

    alog(set_alog_nm(), "[INFO] create_tx_list()")

    try:
        data = []
        for arr in range(0, len(tx_list['result'])):
            if int(tx_list['result'][arr]['value']) != 0 and int(tx_list['result'][arr]['isError']) != 1:
                payout_tx_hash = tx_list['result'][arr]['hash']
                deposit_amount = Decimal(str(tx_list['result'][arr]['value'])) / 1000000000000000000
                payout_dt = tx_list['result'][arr]['timeStamp']

                for x in range(0, len(token_tx['result'])):
                    if token_tx['result'][x]['hash'] == payout_tx_hash:
                        payout_amount = Decimal(str(token_tx['result'][x]['value'])) / 1000000000000000000
                        ether_address = token_tx['result'][x]['to']

                        dict = {
                            "email": "",
                            "user_name": "",
                            "ether_address": ether_address,
                            "payout_tx_hash": payout_tx_hash,
                            "deposit_amount": deposit_amount,
                            "payout_amount": payout_amount,
                            "payout_dt": payout_dt
                        }
                        data.append(dict)

        return data
    except Exception as msg:
        alog(set_alog_nm(), "[ERROR] create_tx_list() - Failed to create data.")
        alog(set_alog_nm(), "[ERROR] create_tx_list() - ERROR MSG = [ %s ]" % (msg))
        sys.exit(0)


def mk_orders_list(db, dbuser, dbpasswd, dbhost, dbcharset, orders_list):

    alog(set_alog_nm(), "[INFO] mk_orders_list()")

    mysqldb = MysqlClass(db, dbuser, dbpasswd, dbhost, dbcharset)
    connect = mysqldb.db_connect()

    alog(set_alog_nm(), "[INFO] mk_orders_list() - Successfully connected to the database.")

    cur = connect[0]
    conn = connect[1]

    data = []

    for e in range(0, len(orders_list)):
        mk_query = "SELECT email, user_name FROM cst_develop.users WHERE ether_address = '%s';" % (orders_list[e]['ether_address'])
        try:
            cur.execute(mk_query)
            rows = cur.fetchall()
            for row in rows:

                values = "('" + row[0] + "','" + row[1] + "','"\
                         + orders_list[e]['ether_address']\
                         + "','" + orders_list[e]['payout_tx_hash']\
                         + "'," + str(orders_list[e]['deposit_amount'])\
                         + "," + str(orders_list[e]['payout_amount'])\
                         + "," + orders_list[e]['payout_dt']\
                         + ")"
                data.append(values)

                alog(set_alog_nm(), "[INFO] EMAIL: [%s]" % (row[0]))
                alog(set_alog_nm(), "[INFO] USER_NAME: [%s]" % (row[1]))
                alog(set_alog_nm(), "[INFO] ETHER_ADRESS: [%s]" % (orders_list[e]['ether_address']))
                alog(set_alog_nm(), "[INFO] PAYOUT_TX_HASH: [%s]" % (orders_list[e]['payout_tx_hash']))
                alog(set_alog_nm(), "[INFO] DEPOSIT_AMOUNT: [%s]" % (orders_list[e]['deposit_amount']))
                alog(set_alog_nm(), "[INFO] PAYOUT_AMOUNT: [%s]" % (orders_list[e]['payout_amount']))
                alog(set_alog_nm(), "[INFO] PAYOUT_DT: [%s]" % (orders_list[e]['payout_dt']))
        except Exception as msg:
            alog(set_alog_nm(), "[ERROR] mk_orders_list() - Failed to create insertion data.")
            alog(set_alog_nm(), "[ERROR] mk_orders_list() - ERROR MSG = [ %s ]" % (msg))
            sys.exit(0)

    values_insert = ', '.join(data)
    insert_query = "INSERT INTO cst_develop.orders (" \
                   "email, user_name, ether_address, payout_tx_hash, deposit_amount, payout_amount, payout_dt" \
                   ") VALUES %s" % (values_insert)
    try:
        cur.execute(insert_query)
        conn.commit()
        return True
    except Exception as msg:
        alog(set_alog_nm(), "[ERROR] mk_orders_list() - Insert statement error occured.")
        alog(set_alog_nm(), "[ERROR] mk_orders_list() - ERROR MSG = [ %s ]" % (msg))
        conn.close()
        sys.exit(0)


def update_start_block(startblock, etherscan_api, inifile, inipath):
    try:
        inifile.set(etherscan_api, 'START_BLOCK', str(startblock))
        with open(inipath + '/bat/config.ini', 'w') as configfile:
            inifile.write(configfile)
        alog(set_alog_nm(), "[INFO] update_start_block() - Overwriting of start block has finished.")
    except Exception as msg:
        alog(set_alog_nm(), "[ERROR] update_start_block() - Failed to overwrite start block.")
        alog(set_alog_nm(), "[ERROR] update_start_block() - ERROR MSG = [ %s ]" % (msg))


def get_file():
    return os.path.split(os.path.realpath(__file__))[1]


def get_path():
    return os.path.split(os.path.realpath(__file__))[0]


def set_alog_nm():
    curr_path = get_path()
    curr_file = get_file()

    if not os.path.isdir(curr_path + '/logs'):
        os.mkdir(curr_path + '/logs')
    curr_dt = datetime.datetime.now().strftime('%m')

    return curr_path + '/logs/' + curr_file.split('.')[0] + '_' + curr_dt + '.log'


def alog(fnm, msg):
    tm_head = "%s" % (time.strftime("[%Y-%m-%d %H:%M:%S] ", time.localtime()))
    t_msg = str(tm_head) + " [" + str(os.getpid()) + "] " + str(msg) + "\n"
    with open(fnm, "a") as FP:
        FP.write(t_msg)


if __name__ == '__main__':
    main()