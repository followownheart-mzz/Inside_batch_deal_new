import importlib
import re

import pandas as pd
import pymysql


class MainCodeMode:
    def __init__(self,df,programId,from_type):
        self.df=df
        self.programId=programId
        self.from_type=from_type
    def get_theme_info(self):
        conn = pymysql.connect(
            host='',
            database='',
            port=,
            user='',
            password='',
            autocommit=True
        )
        cursor = conn.cursor()

        # 获取program信息
        if self.from_type=='基础主题':
            from_theme='acq_theme'
            from_theme_field='acq_theme_field'
        else:
            from_theme = 'ana_theme'
            from_theme_field = 'ana_theme_field'
        sql="""
        select
        a.id,
        a.from_theme_id,
        a.to_theme_id,
        a.process_mode,
        a.from_type,
        b.name as to_theme_name,
        c.program,
        e.field_name
        from (select * from ana_program where id ='{}')a
        left join ana_theme b
        on a.to_theme_id=b.id
        left join {} d
        on a.from_theme_id=d.id
        left join ana_program_code c
        on a.id=c.program_id
        left join {} e
        on a.from_theme_id=e.theme_id
        """.format(self.programId,from_theme,from_theme_field)
        cursor.execute(sql)
        rest=cursor.fetchall()
        self.from_theme_id=rest[0][1]
        self.to_theme_id=rest[0][2]
        self.from_type=rest[0][4]
        self.to_theme_name=rest[0][5]
        self.code=rest[0][6]
        field_name=[]
        for l in rest:
            field_name.append(l[7])
        self.field_name=field_name
    def is_matched(self,pattern, text):
        result = re.match(pattern, text, re.S)
        if result:
            return True
        else:
            return False
    def main(self):
        pd.set_option('mode.chained_assignment', None)
        self.get_theme_info()
        # self.df = self.df[self.field_name]
        regex_pattern = '.*process\(Map<String.*List<Map<String, Object>> list = new ArrayList<>\(\);\s+list.add\(data\);\s+return list.*'
        if_code=self.is_matched(regex_pattern, self.code)
        if if_code:
            return self.df
        else:
            module_name = "business_theme_deal.code_mode.deal._"+self.programId

            # 导入模块
            module = importlib.import_module(module_name)

            # 实例化类
            class_name = "CodeMode"
            CodeMode = getattr(module, class_name)
            self.df['result']=self.df.apply(lambda x:CodeMode(x, 'Inside-Data-Basic-Out', 'Inside-Data-Business-Out').process(), axis=1)
            datas=self.df['result'].sum()
            self.df=pd.DataFrame(datas)
            return self.df






