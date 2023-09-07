import re

import numpy as np
import pandas as pd
import json

from public.database_conn import conn_inside_mysql, close_mysql


class SetCheckRule:
    def __init__(self,theme_id, data, t_check_config, t_theme_field):

        # 输入表和规则json,输出到基础主题库
        self.theme_id=theme_id
        self.data=data
        self.t_check_config=t_check_config
        self.t_theme_field = t_theme_field
        self.str_col=[]
        self.date_col=[]
        self.number_col=[]

    def set_data_type(self):
        # 获取field数据类型
        conn_inside, cursor_inside = conn_inside_mysql()
        sql="select field_name, data_type from ana_theme_field where theme_id='{}'".format(self.theme_id)
        cursor_inside.execute(sql)
        rest=cursor_inside.fetchall()
        close_mysql(conn_inside, cursor_inside)
        field_name_dict={}
        for i in rest:
            field_name_dict[i[0]]=i[1]
        #修改数据类型
        for key,value in field_name_dict.items():
            if value=='数字型':
                try:
                    self.data[key]=self.data[key].astype('float64')
                except:
                    self.data[key]=None
            elif value=='日期型':
                try:
                    self.data[key] = pd.to_datetime(self.data[key],format='%Y-%m-%d %H:%M:%S', errors='coerce')
                except:
                    self.data[key] = None
        return field_name_dict
    def rule_str_comment(self,data):
        comment = np.nan
        for j in self.str_col:
            if not len(str(data[j])):
                comment = (data['comment'] + ';' if not pd.isna(
                    data['comment']) else '') + '字段【' + j + '】自动检查失败'
        for k in self.date_col:
            data[k] = pd.to_datetime(data[k], format='%Y-%m-%d %H:%M:%S')
            if data[k] > pd.Timestamp.now():
                comment = (data['comment'] + ';' if not pd.isna(
                    data['comment']) else '') + '字段【' + k + '】自动检查失败'
        for l in self.number_col:
            if data[l] > 10000000000:
                comment = (data['comment'] + ';' if not pd.isna(
                    data['comment']) else '') + '字段【' + l + '】自动检查失败'
        return comment


    # 设置检查规则标签
    def rule_str_checkResult(self,comment):
        if '失败' in str(comment):
            check_result = '疑似错误'
        else:
            check_result = '检查通过'
        return check_result
    def set_rule(self):
        # 设置数据格式
        field_name_dict=self.set_data_type()
        conn_inside, cursor_inside = conn_inside_mysql()
        # 数据去重
        sql1="""
        select theme_id,id as field_id,field_name,data_type 
        from {} 
        where theme_id = '{}' and if_key='是'""".format(self.t_theme_field,self.theme_id)
        cursor_inside.execute(sql1)
        rest1 = cursor_inside.fetchall()

        df1 = pd.DataFrame(rest1, columns=['theme_id', 'field_id', 'field_name', 'data_type'])
        key_col = df1['field_name'].tolist()
        data = self.data.drop_duplicates(key_col)
        data.index = [i for i in range(len(data))]

        #设置检查规则
        sql = '''
                SELECT  
                a.field_id, 
                b.field_name AS field_name, 
                b.if_key,
                b.data_type,
                a.theme_id,
                a.if_multi,
                a.rule
                FROM {}  a
                LEFT JOIN {} b
                ON a.field_id = b.id
                where a.theme_id='{}'
                ORDER BY a.field_id;
            '''.format(self.t_check_config,self.t_theme_field,self.theme_id)
        cursor_inside.execute(sql)
        rest=cursor_inside.fetchall()
        close_mysql(conn_inside, cursor_inside)
        # 统一规则：关键字段  1.日期不能超过今天2.数值不能大于100亿 3.关键字段不能为空
        rule_theme = pd.DataFrame(rest, columns=['field_id', 'field_name', 'if_key', 'data_type', 'theme_id', 'if_multi', 'rule'])
        rule_theme['if_multi'] = rule_theme['if_multi'].apply(lambda x: int.from_bytes(x, byteorder='big'))
        # 统一规则
        data['checkResult']=np.nan
        data['comment'] = np.nan
        if len(rule_theme)==0:
            self.str_col=df1.loc[df1.data_type=='字符型','field_name'].tolist()
            self.date_col = df1.loc[df1.data_type == '日期型', 'field_name'].tolist()
            self.number_col = df1.loc[df1.data_type == '数字型', 'field_name'].tolist()
            data['comment'] = data.apply(lambda row: self.rule_str_comment(row), axis=1)
            data['checkResult'] = data['comment'].apply(self.rule_str_checkResult)
        # 使用表check_config规则
        else:
            rule_theme.index = [i for i in range(len(rule_theme))]
            for i in range(len(data)):
                try:
                    for j in rule_theme['field_name'].tolist():
                        rule_str = json.loads(rule_theme.loc[rule_theme.field_name == j, 'rule'].tolist()[0])
                        if rule_theme.loc[rule_theme.field_name == j, 'if_multi'].tolist()[0] == 0:
                            if rule_str['rule'] == 'isnull' and pd.isna(data.loc[i, j]):
                                pass
                            elif (rule_str['rule'] == 'notnull' and pd.isna(data.loc[i, j])) \
                                    or (rule_str['rule'] == '<=' and data.loc[i, j] > rule_str['ruleValue']) \
                                    or (rule_str['rule'] == '<' and data.loc[i, j] >= rule_str['ruleValue']) \
                                    or (rule_str['rule'] == '>=' and data.loc[i, j] < rule_str['ruleValue']) \
                                    or (rule_str['rule'] == '>' and data.loc[i, j] <= rule_str['ruleValue']) \
                                    or (rule_str['rule'] == '==' and data.loc[i, j] != rule_str['ruleValue']) \
                                    or (rule_str['rule'] == '!=' and data.loc[i, j] == rule_str['ruleValue']) \
                                    or (rule_str['rule'] == 'beforeNow' and data.loc[i, j] > pd.Timestamp.now()) \
                                    or (rule_str['rule'] == '!contains' and rule_str['ruleValue'] in data.loc[i, j]) \
                                    or (rule_str['rule'] == 'contains' and rule_str['ruleValue'] not in data.loc[i, j]):
                                data.loc[i, 'comment'] = (data.loc[i, 'comment'] + ';' if not pd.isna(
                                    data.loc[i, 'comment']) else '') + '字段【' + j + '】自动检查失败'
                                data.loc[i, 'checkResult'] = '疑似错误'
                        elif not rule_str['ifNot'] and rule_str['groupType'] == 'and':
                            rule_str1 = rule_str['abstractQueries']
                            flag = 1
                            rule_list = [i['rule'] for i in rule_str1]
                            if 'isnull' in rule_list and pd.isna(data.loc[i, j]):
                                pass
                            else:
                                for rule in rule_str1:
                                    if 'ruleValue' in rule:
                                        ruleValue=rule.get('ruleValue')
                                        if field_name_dict[j] == '数字型':
                                            ruleValue = float(ruleValue)
                                        elif field_name_dict[j] == '日期型':
                                            ruleValue=pd.to_datetime(ruleValue,format='%Y-%m-%d %H:%M:%S')
                                        else:
                                            ruleValue=ruleValue
                                    if (rule['rule'] == 'notnull' and pd.isna(data.loc[i, j])) \
                                            or (rule['rule'] == '<=' and data.loc[i, j] > ruleValue) \
                                            or (rule['rule'] == '<' and data.loc[i, j] >= ruleValue) \
                                            or (rule['rule'] == '>=' and data.loc[i, j] < ruleValue) \
                                            or (rule['rule'] == '>' and data.loc[i, j] <= ruleValue) \
                                            or (rule['rule'] == '==' and data.loc[i, j] != ruleValue) \
                                            or (rule['rule'] == '!=' and data.loc[i, j] == ruleValue) \
                                            or (rule['rule'] == 'beforeNow' and data.loc[i, j] > pd.Timestamp.now()) \
                                            or (rule['rule'] == '!contains' and ruleValue in (
                                    '' if pd.isna(data.loc[i, j]) else data.loc[i, j])) \
                                            or (rule['rule'] == 'contains' and ruleValue not in (
                                    '' if pd.isna(data.loc[i, j]) else data.loc[i, j])):
                                        flag = flag & 0
                            if flag == 0:
                                data.loc[i, 'comment'] = (data.loc[i, 'comment'] + ';' if not pd.isna(
                                    data.loc[i, 'comment']) else '') + '字段【' + j + '】自动检查失败'
                                data.loc[i, 'checkResult'] = '疑似错误'
                        elif not rule_str['ifNot'] and rule_str['groupType'] == 'or':
                            rule_str1 = rule_str['abstractQueries']
                            flag = 1
                            rule_list=[i['rule'] for i in rule_str1]
                            if 'isnull' in rule_list and pd.isna(data.loc[i, j]):
                                pass
                            else:
                                for rule in rule_str1:
                                    if 'ruleValue' in rule:
                                        ruleValue=rule.get('ruleValue')
                                        if field_name_dict[j] == '数字型':
                                            ruleValue = float(ruleValue)
                                        elif field_name_dict[j] == '日期型':
                                            ruleValue=pd.to_datetime(ruleValue,format='%Y-%m-%d %H:%M:%S')
                                        else:
                                            ruleValue=ruleValue
                                    if (rule['rule'] == 'notnull' and pd.isna(data.loc[i, j])) \
                                            or (rule['rule'] == '<=' and data.loc[i, j] > ruleValue) \
                                            or (rule['rule'] == '<' and data.loc[i, j] >= ruleValue) \
                                            or (rule['rule'] == '>=' and data.loc[i, j] < ruleValue) \
                                            or (rule['rule'] == '>' and data.loc[i, j] <= ruleValue) \
                                            or (rule['rule'] == '==' and data.loc[i, j] != ruleValue) \
                                            or (rule['rule'] == '!=' and data.loc[i, j] == ruleValue) \
                                            or (rule['rule'] == 'beforeNow' and data.loc[i, j] > pd.Timestamp.now()) \
                                            or (rule['rule'] == '!contains' and ruleValue in (
                                    '' if pd.isna(data.loc[i, j]) else data.loc[i, j])) \
                                            or (rule['rule'] == 'contains' and ruleValue not in (
                                    '' if pd.isna(data.loc[i, j]) else data.loc[i, j])):
                                        flag = flag & 0
                            if flag == 0:
                                data.loc[i, 'comment'] = (data.loc[i, 'comment'] + ';' if not pd.isna(
                                    data.loc[i, 'comment']) else '') + '字段【' + j + '】自动检查失败'
                                data.loc[i, 'checkResult'] = '疑似错误'
                except:
                    data.loc[i,'checkResult']='处理出错'
        data['checkResult'].fillna('检查通过',inplace=True)

        #存储在基础主题库-通过数据
        out=data[data['checkResult'].isin(['检查通过','人工修正']) ]
        # 存储在基础主题库-未通过数据
        result=data[data['checkResult'].isin( ['疑似错误','检查不通过','被人工修正','处理出错'])]
        print('out长度：',len(out))
        print('result长度：', len(result))
        return out,result