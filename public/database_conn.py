# coding: utf-8

import datetime
import json

import pymongo
import pymysql

from public.config import *


def conn_inside_mysql():
    conn_inside = pymysql.connect(
        host='',
        database='',
        port=3306,
        user='',
        password='',
        autocommit=True
    )
    cursor_inside = conn_inside.cursor()
    return conn_inside, cursor_inside


def conn_config_mysql():
    conn_management = pymysql.connect(
        host=task_host,
        database=task_database,
        port=task_port,
        user=task_user,
        password=task_password,
        autocommit=True
    )
    cursor_management = conn_management.cursor()
    return conn_management, cursor_management


def close_mysql(conn, cursor):
    cursor.close()
    conn.close()


def get_query(date_columns, last_worktime):
    if len(date_columns):
        query = {"$and": []}
        for date_col in date_columns:
            date_query_str = '{"$or":[{"' + date_col + '":{"$eq": 1}}, {"' + date_col + '":1}]}'
            date_query = json.loads(date_query_str)
            date_query["$or"][0][date_col]["$eq"] = None
            date_query["$or"][1][date_col] = {"$lt": datetime.datetime(3000, 1, 1),
                                              "$gt": datetime.datetime(1, 1, 1)}
            query["$and"].append(date_query)
        query["$and"].append({'createTime': {'$gt': last_worktime}})
        return query
    else:
        return {}


def get_date_col(theme_field, theme_id):
    sql = """select field_name from {} where data_type='日期型' and theme_id='{}'""".format(theme_field, theme_id)
    conn_inside, cursor_inside = conn_inside_mysql()
    cursor_inside.execute(sql)
    rest = cursor_inside.fetchall()
    close_mysql(conn_inside, cursor_inside)
    date_columns = [i[0] for i in rest]
    return date_columns


def business_conn_mongo(program_id, last_worktime):
    # 获取program信息
    sql = """
    select
    a.id,
    a.from_theme_id,
    a.to_theme_id,
    a.process_mode,
    a.from_type,
    b.name as to_theme_name,
    c.program
    from (select * from ana_program where id='{}')a
    left join ana_theme b
    on a.to_theme_id=b.id
    left join ana_program_code c
    on a.id=c.program_id
    """.format(program_id)
    conn_inside, cursor_inside = conn_inside_mysql()
    cursor_inside.execute(sql)
    rest = cursor_inside.fetchone()
    close_mysql(conn_inside, cursor_inside)

    basic_theme_id = rest[1]
    from_type = rest[4]
    if from_type == '基础主题':
        database = 'Inside-Data-Basic-Out'
        theme_field = 'acq_theme_field'
    else:
        database = 'Inside-Data-Business-Out'
        theme_field = 'ana_theme_field'
    conn = pymongo.MongoClient(inputserver)[database]
    mycol = conn[basic_theme_id]
    date_columns = get_date_col(theme_field, basic_theme_id)
    query = get_query(date_columns, last_worktime)
    return mycol, query


def basic_conn_mongo(theme_id, last_worktime):
    conn = pymongo.MongoClient(inputserver)['Inside-Data-Basic-Out']
    mycol = conn[theme_id]
    date_columns = get_date_col('acq_theme_field', theme_id)
    query = get_query(date_columns, last_worktime)
    return mycol, query
