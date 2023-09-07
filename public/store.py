import os
import pymongo
from public.config import *

def output_excel(out, result, theme_name, output_type):
    if output_type == '基础主题':
        path = r'G:\工作_马转转\文档\python\pycharm_project\inside_batch_deal\basic\basic_theme_out\\'
    else:
        path = r'G:\工作_马转转\文档\python\pycharm_project\inside_batch_deal\business\business_theme_out\\'
    pass_filename = os.path.join(path, 'pass', theme_name + '.csv')
    no_pass_filename = os.path.join(path, 'nopass', theme_name + '.csv')

    if not os.path.isfile(pass_filename):
        out.to_csv(pass_filename, mode='a', index=False, header=True, encoding='utf_8_sig')
    else:
        out.to_csv(pass_filename, mode='a', index=False, header=False, encoding='utf_8_sig')

    if not os.path.isfile(no_pass_filename):
        result.to_csv(no_pass_filename, mode='a', index=False, header=True, encoding='utf_8_sig')
    else:
        result.to_csv(no_pass_filename, mode='a', index=False, header=False, encoding='utf_8_sig')

def output_mongodb(out, result, theme_id, output_type):
    # 连接 MongoDB
    client = pymongo.MongoClient(outputserver)
    if output_type == '基础主题':
        db_out = client['']
        db_result = client['']
    else:
        db_out = client['']
        db_result = client['']

    collection_out = db_out[theme_id]
    collection_result = db_result[theme_id]

    # 存储通过数据
    collection_out.insert_one(out.to_dict('records'))
    # 存储未通过数据
    collection_result.insert_one(result.to_dict('records'))
