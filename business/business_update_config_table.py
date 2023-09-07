# coding: utf-8

import pandas as pd

from public.get_program_info import *


class ModifyManageTable:
    def __init__(self, program_id):
        self.program_id = program_id
        self.t_task_management = 'business_task_management'
        self.t_batch_management = 'business_batch_management'
        self.task_id = None
        self.error = ''
        rest0 = get_info(self.program_id)
        self.program_name = rest0[1]
        self.from_theme_name = rest0[2]
        self.to_theme_name = rest0[3]
        self.from_theme_id = rest0[4]
        self.to_theme_id = rest0[5]
        self.process_mode = rest0[6]
        self.from_type = rest0[7]
        self.task_name = self.program_name
        self.number_of_times = None

    # 获取上次导出时间和taskId
    def get_last_last_worktime_task_id(self):
        # 创建或更新任务表
        conn_management, cursor_management = conn_config_mysql()
        last_work_time = datetime.datetime(9999, 1, 1)
        try:
            dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            sql = "select * from {} where programId='{}';".format(self.t_task_management, self.program_id)
            sql1 = "insert into {} (taskName,programId,programName,fromThemeId,fromThemeName,toThemeId,toThemeName,fromType,processMode,createTime,status) values('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}');" \
                .format(self.t_task_management, self.task_name, self.program_id, self.program_name, self.from_theme_id,
                        self.from_theme_name, self.to_theme_id, self.to_theme_name, self.from_type, self.process_mode, dt, '正常')

            sql2 = "select lastWorkTime,id as taskId from {} where programId='{}';".format(self.t_task_management,
                                                                                           self.program_id)
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
            sql3 = "select numberOfTimes from {} where taskId={};".format(self.t_batch_management, self.task_id)
            cursor_management.execute(sql3)
            rest3 = cursor_management.fetchall()

            if len(rest3) == 0:
                self.number_of_times = 1
            else:
                # 获取任务批次表的maxNumberOfTimes
                self.number_of_times = pd.DataFrame(rest3)[0].max() + 1

            sql4 = "insert into {} (taskId,batchName,createTime,numberOfTimes,batchStatus) values({},'{}','{}',{},'{}')" \
                .format(self.t_batch_management, self.task_id, self.task_name + '-第' + str(self.number_of_times) + '批次', dt,
                        self.number_of_times, '正在运行')
            cursor_management.execute(sql4)

            # 更新 LatestBatchCreateTime
            sql5 = "update {} set LatestBatchCreateTime='{}' where id={}".format(self.t_task_management, dt, self.task_id)
            cursor_management.execute(sql5)

        except Exception as e:
            print('获取新增数据失败:', e.args)
            self.error = self.error + '业务主题-创建更新任务表失败-' + str(e.args)
        close_mysql(conn_management, cursor_management)
        return last_work_time, self.number_of_times

    # 更新任务表和批次表
    def update_task_batch(self, max_create_time, min_create_time, last_batch_volume):
        conn_management, cursor_management = conn_config_mysql()
        if min_create_time:
            # 获取batchVolume
            sql0 = "select sum(case when batchVolume is null then 0 else batchVolume end) as totalVolume from {} where taskId={}".format(
                self.t_batch_management, self.task_id)
            cursor_management.execute(sql0)
            total_volume = int(cursor_management.fetchone()[0])
            batch_volume = total_volume + last_batch_volume

            # 更新任务表
            sql1 = "update {} set lastWorkTime='{}',totalVolume={} where id={};".format(self.t_task_management, max_create_time,
                                                                                        batch_volume, self.task_id)
            cursor_management.execute(sql1)

            sql2 = "update {} set lastWorkTime='{}',earlyWorkTime='{}',endTime='{}',batchVolume={},batchStatus='{}' where taskId={} and numberOfTimes={}" \
                .format(self.t_batch_management, max_create_time, min_create_time,
                        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), last_batch_volume, "运行成功", self.task_id,
                        self.number_of_times)
            cursor_management.execute(sql2)
        else:
            sql3 = "update {} set endTime='{}',batchVolume={},batchStatus='{}' where taskId={} and numberOfTimes={}" \
                .format(self.t_batch_management, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), 0, "运行成功",
                        self.task_id, self.number_of_times)
            cursor_management.execute(sql3)
        close_mysql(conn_management, cursor_management)

    def batch_exception(self):
        conn_management, cursor_management = conn_config_mysql()
        sql = "update {} set endTime='{}',batchStatus='{}' where taskId={} and numberOfTimes={}" \
            .format(self.t_batch_management, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), "运行失败", self.task_id,self.number_of_times)
        cursor_management.execute(sql)
        close_mysql(conn_management, cursor_management)

    def get_batch_id(self):
        conn_management, cursor_management = conn_config_mysql()
        sql = "select id as batchId from {} where taskId={} and numberOfTimes={}" \
            .format(self.t_batch_management,self.task_id, self.number_of_times)
        cursor_management.execute(sql)
        batch_id = cursor_management.fetchone()[0]
        close_mysql(conn_management, cursor_management)
        return batch_id
