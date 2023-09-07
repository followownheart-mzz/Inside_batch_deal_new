import pandas as pd
import pymysql


class Rules:
    def __init__(self, data):
        self.data = data

    def date_number_nan(self):
        '''
        把日期型和数字型的字段为空的放入未通过数据
        '''
        columns = self.data.columns
        result = pd.DataFrame()
        out = self.data
        for col in columns:
            if self.data[col].dtype == 'object':
                result1 = out[out[col].isnull()]
                out = out[out[col].notnull()]
            elif self.data[col].dtype == 'datetime64[ns]':
                result1 = out[out[col].isnull()]
                out = out[out[col].notnull()]
            else:
                result1 = pd.DataFrame()
            result = pd.concat([result, result1], axis=0)
        return result, out

    def get_key_field(self, theme_field, theme_id):
        # 获取关键字段
        conn = pymysql.connect(
            host='192.168.2.230',
            database='inside',
            port=3306,
            user='datagroup01',
            password='datagroup01',
            autocommit=True
        )
        cursor = conn.cursor()
        sql = "select field_name from {} where theme_id='{}' and if_key='是'".format(theme_field, theme_id)
        cursor.execute(sql)
        rest = cursor.fetchall()
        field_name_list = [i[0] for i in rest]
        return field_name_list

    def full_field_deduplication(self):
        '''
        全字段去重
        :return:
        '''
        self.data.sort_values('createTime', ascending=False, inplace=True)
        self.data.drop_duplicates(inplace=True, keep='first')
        return self.data

    def key_field_deduplication(self, theme_field, theme_id):
        '''
        关键字段去重
        :return:
        '''
        # 获取关键字段
        field_name_list = self.get_key_field(theme_field, theme_id)

        # 删除关键字段重复行，保留createTime最大的那条数据
        self.data.sort_values('createTime', ascending=False, inplace=True)

        self.data.drop_duplicates(subset=field_name_list, inplace=True, keep='first')

        return self.data

    def batch_concat_deduplication(self, theme_field, theme_id):
        '''
        批次合并去重：①新数据和旧数据全字段相同，新数据不入合并表②关键字段相同，只保留最新日期数据
        :param theme_field:
        :param theme_id:
        :return:
        '''
        # 获取关键字段
        field_name_list = self.get_key_field(theme_field, theme_id)
