# -------- OPTIONS --------
API_URL_BASE = 'https://<SOME_DOMAIN>/'
HEADERS = {
    'x-auth-token': '<SOME_TOKEN>',
    'x-auth-user': '<SOME_USER>'
}
DB_NAME = '<SOME_DB_NAME>'
DB_USER = '<SOME_DB_USER_NAME>'
DB_PASSWORD = '<SOME_DB_PASSWORD>'
# -------------------------


import requests
import json
import pymysql.cursors
from datetime import datetime
from decimal import *
import sys
import time

# -------- INITIALIZATION --------
start_at = datetime.now()
updatable_count = 0
updated_count = 0
updated_duplicate = 0
page_num = 1
updatable = []
after_date = None

def _now_str():
    return datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")

def get_products_by_page():
    global API_URL_BASE
    global after_date
    global page_num
    response = requests.get(
        '{0}<GET_GOODS_BY_FILTER_METHOD_NAME>?sort=<UPDATED_AT_FIELD>&page={1}'.format(API_URL_BASE, str(page_num)), 
        headers=HEADERS
    )
    if response.status_code == 200:
        data = json.loads(response.content.decode('utf-8'))
        max_key = 0
        for key in data.keys():
            try:
                _key = int(key)
                if _key > max_key:
                    max_key = _key
            except ValueError:
                continue
        for i in range(1, max_key + 1):
            _i = str(i)
            if data[_i]['<UPDATED_AT_FIELD>'] > after_date:
                yield '{0},{1},{2},{3}'.format(
                    data[_i]['<ID_FIELD>'], 
                    data[_i]['<PRICE_FIELD>'], 
                    data[_i]['<QUANTITY_FIELD>'], 
                    data[_i]['<UPDATED_AT_FIELD>']
                )
            else:
                break
        else:
            page_num += 1
            _get_products_by_page()
    else:
        raise Exception('Response with status {0} recieved when get {1} page from API'.format(response.status_code, page_num))

def _get_products_by_page():
    global updatable_count
    global updated_count
    global updated_duplicate
    global updatable
    if updatable_count != 0:
        print('[{0}] Received {1} items for upgrade'.format(_now_str(), updatable_count))
    for row in get_products_by_page():
        updatable_count += 1
        updatable.append(row)

# -------- START PROCESS ---------
print('Requesting the date and time of the last update of prices and balances...')
_connection = pymysql.connect(host='localhost',
                              user=DB_USER,
                              password=DB_PASSWORD,
                              db=DB_NAME,
                              charset='utf8mb4')
try:
    with _connection.cursor() as cursor:
        result = cursor.execute(
            "SHOW TABLES FROM {0} LIKE 'goods_updater';".format(DB_NAME)
        )
        if result == 0:
            cursor.execute("""
                CREATE TABLE `goods_updater` (
                `key` VARCHAR(255) NOT NULL,
                `value` VARCHAR(255),
                PRIMARY KEY (`key`)
                );
            """)
        cursor.execute("""
            SELECT `value` FROM `goods_updater` WHERE `key`='last_udated_at';
        """)
        _after_date = cursor.fetchone()
        after_date = _after_date[0] if _after_date else None
        cursor.execute("""
            INSERT INTO `goods_updater` (`key`, `value`)
            VALUES ('last_udated_at', '{0}')
            ON DUPLICATE KEY UPDATE `value`='{0}'
        """.format(_now_str()))
    _connection.commit()
finally:
    _connection.close()
if not after_date:
    print('The date and time of last update not found\nGetting a full list of product updates...')
    response = requests.get(
        '{0}<GET_ALL_GOODS_BY_FILE_METHOD_NAME>/?&fields=<ID_FIELD>,<PRICE_FIELD>,<QUNTITY_FIELD>,<UPDATED_AT_FIELD>'.format(API_URL_BASE), 
        headers=HEADERS
    )
    if response.status_code == 200:
        data = json.loads(response.content.decode('utf-8'))
        if data['result'] != 'success':
            print(data['message'])
            raise
        time.sleep(8)
        _response = requests.get('{0}<GET_FILE_METHOD_NAME>/{1}'.format(API_URL_BASE, data['export_file']), headers=HEADERS)
        if _response.status_code == 200:
            _data = json.loads(_response.content.decode('utf-8'))
            __response = requests.get(_data['url'])
            if __response.status_code == 200:
                updatable = __response.content.decode('utf-8').split('\n')[1:-1]
                updatable_count = len(updatable)
else:
    print('The date and tim of the last update {0}\nSearching of a new product updates...'.format(after_date))
    _get_products_by_page()
if updatable_count != 0:
    print('Received a list of data on the products being updated: {0} items for upgrade'.format(updatable_count))
    connection = pymysql.connect(host='localhost',
                                 user=DB_USER,
                                 password=DB_PASSWORD,
                                 db=DB_NAME,
                                 charset='utf8mb4')
    print('Creating transaction...')
    i = 0
    for _row in updatable:
        i+=1
        row = _row.split(',')
        if i%500==0:
            print('   Processed {0} records out of {1}'.format(i, updatable_count))
        try:
            with connection.cursor() as cursor:
                result = cursor.execute("""
                    UPDATE products
                    SET products.<PRICE_FIELD>={0}, products.<QUNTITY_FIELD>={1}, product.<UPDATED_AT_FIELD>='{2}' 
                    WHERE oc_product.<ID_FIELD>='{3}'
                    """.format(row[1],
                               row[2], 
                               _now_str(),
                               row[0])
                )
                if result:
                    updated_count += 1
                    if result > 1:
                        updated_duplicate += 1
        except:
            connection.close()
            print('Unexpected error:', sys.exc_info()[0])
            raise
    print('Updating of the prices and balances...')
    connection.commit()
    connection.close()
else:
    print('The new updating not found')
print(
    'Updating of the prices and balances is completed:\n'
    ' - received {0} items for upgrade\n'.format(updatable_count), 
    '- found and updated {0} items\n'.format(updated_count), 
    '- found {0} duplicate items by SKU\n'.format(updated_duplicate),
    '- it took time: {0}'.format(str(datetime.now() - start_at))
)