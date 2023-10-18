from basic.basic_get_additions import *
from basic.basic_update_config_table import ModifyManageTable
from public.set_check_rule import *
import pandas as pd
from public.store import output_excel
import threading
import os
from public.database_conn import *

# 获取基础主题id（指定基础主题名称）
def get_deliver_resource():
    deliver_resource = pd.read_excel(r"交付数据源.xlsx", sheet_name="Sheet4")['name'].tolist()
    deliver_resource_id = []
    conn_inside, cursor_inside = conn_inside_mysql()
    for theme_name in deliver_resource:
        sql = "select id from acq_theme where name ='{}'".format(theme_name)
        cursor_inside.execute(sql)
        deliver_resource_id.append(cursor_inside.fetchone()[0])
    close_mysql(conn_inside, cursor_inside)
    return deliver_resource_id

def consumer(queue):
    while True:
        # 从队列中获取数据
        item = queue.get()
        if item is None:
            break  # 获取到空值时，表示所有任务已完成，线程退出
        process_item(item)

def yield_rows(cursor, chunk_size):
    chunk = []
    for i, row in enumerate(cursor):
        if i % chunk_size == 0 and i > 0:
            yield chunk
            del chunk[:]
        chunk.append(row)
    yield chunk

# 定义处理数据的函数
def process_item(item):
    global batch_id, max_create_time, min_create_time
    basic_theme_id = item
    error = ''
    data_volume = 0

    # 批次表新增批次信息
    config_manage = ModifyManageTable(basic_theme_id)
    basic_theme_name, last_work_time, task_id, number_of_times = config_manage.get_last_lastworktime_taskid()

    # 设置批次读取数据
    CHUNK_SIZE = 10000  # 一次读取数据的条数
    mycol, query = basic_conn_mongo(basic_theme_id, last_work_time)

    cursor = mycol.find(query, batch_size=CHUNK_SIZE)
    chunks = yield_rows(cursor, CHUNK_SIZE)
    for chunk in chunks:
        data = pd.DataFrame(chunk)

    # 获取新增数据
        try:
            df, max_create_time, min_create_time, last_batch_volume = GetNewAdditions(data, basic_theme_id, number_of_times).get_data()
            data_volume = data_volume + last_batch_volume
            print(basic_theme_name, '获取新增数据成功')
        except Exception as e:
            error = error + '基础主题-获取新增数据失败-' + str(e.args) + ';'
            print(error)
            data = pd.DataFrame({})

        if len(data):
            # 设置基础主题检查规则
            try:
                out, result = SetCheckRule(basic_theme_id, data, 'acq_check_config', 'acq_theme_field').set_rule()
                print(basic_theme_name, '设置基础主题检查规则成功')
            except Exception as e:
                error = error + '基础主题-设置基础主题检查规则失败-' + str(e.args) + ';'
                print(error)
                out = pd.DataFrame({})
                result = pd.DataFrame({})

            # 存储基础主题数据
            try:
                output_excel(out, result, basic_theme_name, '基础主题')
                print(basic_theme_name, '存储基础主题数据成功')
            except Exception as e:
                error = error + '基础主题-存储基础主题数据失败-' + str(e.args) + ';'
                print(error)
        else:
            print('基础主题无新增数据')

    # 更新配置表
    if error:
        config_manage.batch_exception()
    else:
        config_manage.update_task_batch(max_create_time, min_create_time, data_volume)

    # 存储异常信息
    batch_id = config_manage.get_batch_id()
    if len(error):
        error_info = [{'batchId': batch_id, 'error': error, 'createTime': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")}]
        error_df = pd.DataFrame(error_info)
        filename = r'error.csv'
        if not os.path.isfile(filename):
            error_df.to_csv(filename, mode='a', index=False, header=True)
        else:
            error_df.to_csv(filename, mode='a', index=False, header=False)

def main():
    import queue
    deliver_resource = get_deliver_resource()
    # 创建一个线程安全的队列
    queue = queue.Queue()

    # 向队列中添加数据
    for item in deliver_resource:
        queue.put(item)

    # 创建多个消费者线程
    num_consumers = 3  # 消费者线程数量
    for _ in range(num_consumers):
        queue.put(None)  # 添加停止信号
    consumers = []
    for _ in range(num_consumers):
        t = threading.Thread(target=consumer, args=(queue,))
        consumers.append(t)

    # 启动消费者线程
    for t in consumers:
        t.start()

    # 等待所有消费者线程结束
    for t in consumers:
        t.join()

    # 所有任务处理完成，程序结束

if __name__ == '__main__':
    main()
