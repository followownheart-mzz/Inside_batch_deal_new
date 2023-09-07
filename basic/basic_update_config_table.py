import pandas as pd
from public.database_conn import *

class ModifyManageTable:
    def __init__(self, theme_id):
        self.theme_id = theme_id
        self.data_source_id = None
        self.data_source_name = None
        self.task_id = None
        self.number_of_times = None

        # 获取主题信息
        conn_inside, cursor_inside = conn_inside_mysql()
        sql0 = "SELECT a.name AS basic_theme_id, b.id AS source_id, b.name AS source_name FROM acq_theme a LEFT JOIN acq_source b ON a.id = b.theme_id WHERE a.id = %s"
        cursor_inside.execute(sql0, (self.theme_id))
        rest0 = cursor_inside.fetchone()
        self.theme_name = rest0[0]
        self.task_name = rest0[0]
        self.data_source_id = rest0[1]
        self.data_source_name = rest0[2]

        close_mysql(conn_inside, cursor_inside)

    def get_last_lastworktime_taskid(self):
        conn_management, cursor_management = conn_config_mysql()
        dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        sql = "SELECT * FROM basic_task_management WHERE themeName = '{}';".format(self.theme_name)
        sql1 = "INSERT INTO basic_task_management (taskName, themeName, themeId, createTime, status, dataSourceId, dataSourceName) VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}');" \
            .format(self.task_name, self.theme_name, self.theme_id, dt, '正常', self.data_source_id, self.data_source_name)
        sql2 = "SELECT lastWorkTime, id AS taskId FROM basic_task_management WHERE themeName = '{}';".format(self.theme_name)

        # 检查task表中是否有该任务，若没有插入数据
        cursor_management.execute(sql)
        rest = cursor_management.fetchall()

        if len(rest) == 0:
            cursor_management.execute(sql1)

        # 获取lastWorkTime和 taskId
        cursor_management.execute(sql2)
        rest2 = cursor_management.fetchall()
        last_work_time = rest2[0][0]
        self.task_id = rest2[0][1]

        # 增加任务批次记录
        sql3 = "SELECT numberOfTimes FROM basic_batch_management WHERE taskId = {};".format(self.task_id)
        cursor_management.execute(sql3)
        rest3 = cursor_management.fetchall()

        if len(rest3) == 0:
            self.number_of_times = 1
        else:
            # 获取任务批次表的maxNumberOfTimes
            self.number_of_times = pd.DataFrame(rest3)[0].max() + 1

        sql4 = "INSERT INTO basic_batch_management (taskId, batchName, createTime, numberOfTimes, batchStatus) VALUES ({}, '{}', '{}', {}, '{}')" \
            .format(self.task_id, self.task_name + '-第' + str(self.number_of_times) + '批次', dt, self.number_of_times, '正在运行')
        cursor_management.execute(sql4)

        # 更新 LatestBatchCreateTime
        sql5 = "UPDATE basic_task_management SET LatestBatchCreateTime = '{}' WHERE id = {}".format(dt, self.task_id)
        cursor_management.execute(sql5)

        close_mysql(conn_management, cursor_management)
        return self.theme_name, last_work_time, self.task_id, self.number_of_times

    def update_task_batch(self, max_create_time, min_create_time, last_batch_volume):
        conn_management, cursor_management = conn_config_mysql()
        if min_create_time:
            # 获取batchVolume
            sql0 = "SELECT SUM(CASE WHEN batchVolume IS NULL THEN 0 ELSE batchVolume END) AS totalVolume FROM basic_batch_management WHERE taskId = {}".format(self.task_id)
            cursor_management.execute(sql0)
            total_volume = int(cursor_management.fetchone()[0])
            batch_volume = total_volume + last_batch_volume

            # 更新任务表
            sql1 = "UPDATE basic_task_management SET lastWorkTime = '{}', totalVolume = {} WHERE id = {};".format(max_create_time, batch_volume, self.task_id)
            cursor_management.execute(sql1)

            sql2 = "UPDATE basic_batch_management SET lastWorkTime = '{}', earlyWorkTime = '{}', endTime = '{}', batchVolume = {}, batchStatus = '{}' WHERE taskId = {} AND numberOfTimes = {}" \
                .format(max_create_time, min_create_time, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), last_batch_volume, "运行成功", self.task_id, self.number_of_times)
            cursor_management.execute(sql2)
        else:
            sql3 = "UPDATE basic_batch_management SET endTime = '{}', batchVolume = {}, batchStatus = '{}' WHERE taskId = {} AND numberOfTimes = {}" \
                .format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), 0, "运行成功", self.task_id, self.number_of_times)
            cursor_management.execute(sql3)
        close_mysql(conn_management, cursor_management)

    def batch_exception(self):
        conn_management, cursor_management = conn_config_mysql()
        sql = "UPDATE basic_batch_management SET endTime = '{}', batchStatus = '{}' WHERE taskId = {} AND numberOfTimes = {}" \
            .format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), "运行失败", self.task_id, self.number_of_times)
        cursor_management.execute(sql)
        close_mysql(conn_management, cursor_management)

    def get_batch_id(self):
        conn_management, cursor_management = conn_config_mysql()
        sql = "SELECT id AS batchId FROM basic_batch_management WHERE taskId = {} AND numberOfTimes = {}" \
            .format(self.task_id, self.number_of_times)
        cursor_management.execute(sql)
        batch_id = cursor_management.fetchone()[0]
        close_mysql(conn_management, cursor_management)
        return batch_id
