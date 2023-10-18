
import datetime
import pandas as pd

class GetNewAdditions:
    def __init__(self, df, theme_id, number_of_times):
        self.df = df
        self.theme_id = theme_id
        self.number_of_times = number_of_times

    # 获取上次导出时间和taskId
    def get_data(self):
        global min_create_time
        last_batch_volume = len(self.df)
        if last_batch_volume > 0:
            max_create_time = self.df['createTime'].max()
            min_create_time = self.df['createTime'].min()
        else:
            max_create_time = None
            min_create_time = None

        self.add_column(self.df)
        return self.df, max_create_time, min_create_time, last_batch_volume

    # 更新任务表和批次表
    def add_column(self, df):
        df['basicCreateTime'] = datetime.datetime.now()
        df['basicUpdateTime'] = datetime.datetime.now()
        df['basicBatchNumber'] = self.number_of_times
        return df
