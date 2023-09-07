import os
import threading
from queue import Queue

from business.business_get_additions import *
from business.business_update_config_table import *
from public.database_conn import *
from public.overall_rules import *
from public.set_check_rule import *
from public.store import output_excel


# 获取业务程序
def get_ana_process():
    # 这里把from_type为基础主题和业务主题的分别存入两个列表的原因是：先执行from_type为基础主题的，再执行业务主题的，因为from_type='业务主题'的需要用到from_type='基础主题'
    id_list_basic = ["a4e33c480d371c38ba336bbed27166eb"]
    id_list_business = []
    id_list = id_list_basic + id_list_business
    return id_list


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


def get_check_mode(to_theme_id):
    sql = "select check_mode from ana_theme where id='{}'".format(to_theme_id)
    conn_inside, cursor_inside = conn_inside_mysql()
    cursor_inside.execute(sql)
    check_mode = cursor_inside.fetchone()[0]
    close_mysql(conn_inside, cursor_inside)
    return check_mode


def process_item(item):
    global batch_id, program_name, to_theme_id, to_theme_name, check_mode, max_create_time, min_create_time, last_batch_volume
    program_id = item
    print('*' * 80, program_id)
    data_volume = 0
    error = ''

    # 批次表新增批次信息
    config_manage = ModifyManageTable(program_id)
    last_work_time, number_of_times = config_manage.get_last_last_worktime_task_id()
    program_info = get_info(program_id)
    to_theme_id = program_info[5]
    program_name = program_info[1]
    to_theme_name = program_info[3]
    check_mode = get_check_mode(to_theme_id)

    # 设置批次读取数据
    CHUNK_SIZE = 10000  # 一次读取数据的条数
    my_col, query = business_conn_mongo(program_id, last_work_time)
    cursor = my_col.find(query, batch_size=CHUNK_SIZE)
    chunks = yield_rows(cursor, CHUNK_SIZE)

    for chunk in chunks:
        data = pd.DataFrame(chunk)
        # 获取新增数据
        try:
            data, max_create_time, min_create_time, last_batch_volume = GetNewAdditions(program_id, data,
                                                                                      number_of_times).get_data()
            data_volume = data_volume + last_batch_volume
            print('获取业务主题数据成功')
        except Exception as e:
            error = error + '业务主题-获取新增数据失败-' + str(e.args) + ';'
            print(error)
            data = pd.DataFrame({})

        # 检查数据
        if data is not None:
            if len(data):
                # 设置业务主题检查规则
                try:
                    if check_mode != '全部通过':
                        out, result = SetCheckRule(to_theme_id, data, 'ana_check_config', 'ana_theme_field').set_rule()
                        print(program_name, '设置业务主题检查规则成功')
                    else:
                        data['checkResult'] = '检查通过'
                        data['comment'] = np.nan
                        out = data
                        result = pd.DataFrame()
                except Exception as e:
                    error = error + '业务主题-设置业务主题检查规则失败-' + str(e.args) + ';'
                    print(error)
                    out = pd.DataFrame({})
                    result = pd.DataFrame({})

                # 关键字段去重
                if len(out):
                    out = Rules(out).key_field_deduplication('ana_theme_field', to_theme_id)

                # 存储业务主题数据
                try:
                    output_excel(out, result, to_theme_name, '业务主题')
                    print(program_id, '-', program_name, '存储业务主题数据成功')
                except Exception as e:
                    error = error + '业务主题-存储业务主题数据失败-' + str(e.args) + ';'
                    print(error)
            else:
                print('业务主题无新增数据')
        else:
            print('业务主题无新增数据')

    # 更新配置表
    if error:
        config_manage.batch_exception()
    else:
        config_manage.update_task_batch(max_create_time, min_create_time, data_volume)

    # 获取错误信息
    batch_id = config_manage.get_batch_id()
    if error is not None or error != '':
        error_info = [{'batchId': batch_id, 'error': error,
                       'createTime': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")}]
        error_df = pd.DataFrame(error_info)
        filename = r'error.csv'
        if not os.path.isfile(filename):
            error_df.to_csv(filename, mode='a', index=False, header=True)
        else:
            error_df.to_csv(filename, mode='a', index=False, header=False)


def main():
    deliver_resource = get_ana_process()
    # 创建一个线程安全的队列
    queue = Queue()

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


if __name__ == '__main__':
    main()
    # deliver_resource = get_ana_process()[::-1]
    # while True:
    #     # 从队列中获取数据
    #
    #     if not deliver_resource :
    #         break  # 获取到空值时，表示所有任务已完成，线程退出
    #     item = deliver_resource.pop()
    #     process_item(item)
