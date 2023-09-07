import datetime
import json
import re

import numpy as np
import pymongo

from public.config import *
from public.overall_rules import *

pd.set_option('display.max_columns', None)   # 显示完整的列
pd.set_option('display.max_rows', None)  # 显示完整的行
pd.set_option('display.expand_frame_repr', False)  # 设置不折叠数据



class Relation:
    def __init__(self,df,programId):
        self.df=df
        self.programId=programId
        self.conn = pymysql.connect(
            host='192.168.2.230',
            database='inside',
            port=3306,
            user='datagroup01',
            password='datagroup01',
            autocommit=True
        )
        self.cursor = self.conn.cursor()
    def get_field_name(self,theme_id,field_id,theme_type):
        if theme_type=='基础主题':
            theme_field_table='acq_theme_field'
        else:
            theme_field_table = 'ana_theme_field'
        sql="select field_name from {} where theme_id='{}' and id='{}'".format(theme_field_table,theme_id,field_id)
        self.cursor.execute(sql)
        field_name=self.cursor.fetchone()[0]
        return field_name
    def filter(self,row,_type,theme_id,theme_type):
        if _type in self.config:
            filter_list=self.config[_type]['filterList']
            for filter in filter_list:
                if filter['ifMulti']==False:
                    rule=filter['rule']
                    if rule['rule']=='==':
                        rule_value =rule['ruleValue']
                        field_id=filter['fieldId']
                        field_name=self.get_field_name(theme_id,field_id,theme_type)
                        if row[field_name]!=rule_value:
                            return 1
                    elif rule['rule']=='!=':
                        rule_value=rule['ruleValue']
                        field_id = filter['fieldId']
                        field_name = self.get_field_name(theme_id, field_id,theme_type)
                        if row[field_name]==rule_value:
                            return 1
                    elif rule['rule']=='contains':
                        rule_value=rule['ruleValue']
                        field_id = filter['fieldId']
                        field_name = self.get_field_name(theme_id, field_id,theme_type)
                        if rule_value not in row[field_name]:
                            return 1
                    elif rule['rule']=='!contains':
                        rule_value=rule['ruleValue']
                        field_id = filter['fieldId']
                        field_name = self.get_field_name(theme_id, field_id,theme_type)
                        if rule_value  in row[field_name]:
                            return 1
                    elif rule['rule']=='notnull':
                        field_id = filter['fieldId']
                        field_name = self.get_field_name(theme_id, field_id,theme_type)
                        if  len('' if pd.isna(row[field_name]) else list(str(row[field_name])))==0:
                            return 1
                    elif rule['rule']=='isnull':
                        field_id = filter['fieldId']
                        field_name = self.get_field_name(theme_id, field_id,theme_type)
                        if len('' if pd.isna(row[field_name]) else list(str(row[field_name])))>0:
                            return 1
        return 0
    def set_column_name(self,df1,df2,df_merge,left_on):
        common_columns = np.intersect1d(df1.columns, df2.columns)
        if len(common_columns):
            no_key_columns = [i for i in common_columns if i not in left_on]
            for no_key_column in no_key_columns:
                df_merge.rename(columns={no_key_column + '_x': no_key_column}, inplace=True)
        return df_merge
    def logic_type_or(self,df1,df2,left_on,right_on):
        df1_relation_key = ['key1', 'key2', 'key4']
        common_columns = np.intersect1d(df1.columns, df2.columns)
        drop_columns = [i for i in df1_relation_key if i != 'key1']
        df_columns = [i for i in common_columns if i in drop_columns]
        df4 = df2.drop(df_columns, axis=1)
        merge_key1 = pd.merge(df1, df4,left_on=left_on, right_on=right_on,how='inner')
        return merge_key1
    def get_theme_data(self,theme_id,to_type):
        if  to_type=='基础主题':
            conn = pymongo.MongoClient(inputserver)[inputdatabase]
        else:
            conn = pymongo.MongoClient(inputserver)[inputdatabase1]
        mycol = conn[theme_id]
        cursor=mycol.find({})
        df = pd.DataFrame(list(cursor))
        return df


    def get_calculate_col(self,expression):
        regex = r"[-+*/]"  # 包含分隔符的正则表达式
        return re.split(regex, expression)

    def calculate(self,x, expression):
        calculate_cols = self.get_calculate_col(expression)

        for i in calculate_cols:
            expression = expression.replace(i, str(x[i]))
        try:
            return eval(expression)
        except ZeroDivisionError:
            return "Error: Division by zero"
        except Exception as e:
            return None
    def agg_cal(self,x, cal_col):
        # 计算createTime列
        if cal_col=='createTime':
            return x[cal_col].max()
        x.index = [i for i in range(len(x))]
        cal_col_sort = cal_col + '_sort'
        rule = x.loc[0, cal_col_sort]
        cal_col = cal_col + '_' + rule['themeId']
        value = None
        if (rule['groupType'] == 'last' and rule['sortType'] == '升序') or (
                rule['groupType'] == 'first' and rule['sortType'] == '降序'):
            index=int(x.sort_values(by=[rule['sortFieldName'] + '_' + rule['themeId']],ascending=False).iloc[0].name)
            value = x.loc[index, cal_col]
        elif (rule['groupType'] == 'last' and rule['sortType'] == '降序') or (
                rule['groupType'] == 'first' and rule['sortType'] == '升序'):
            index=int(x.sort_values(by=[rule['sortFieldName'] + '_' + rule['themeId']],ascending=True).iloc[0].name)
            value = x.loc[index, cal_col]
        elif rule['groupType'] == 'sum':
            value = x[cal_col].sum()
        elif rule['groupType'] == 'count':
            value = x[cal_col].count()
        elif rule['groupType'] == 'avg':
            value = x[cal_col].avg()
        elif rule['groupType'] == 'min':
            value = x[cal_col].min()
        elif rule['groupType'] == 'max':
            value = x[cal_col].max()
        return value
    def add_lose_cols(self,theme_id):
        key_field_list=Rules(self.df).get_key_field('ana_theme_field', theme_id)
        df_columns=list(self.df.columns)
        lose_cols=[col for col in key_field_list if col not in df_columns]
        for col in lose_cols:
            self.df[col]=None
    def main(self):
        if self.df is not None:
            # 获取program信息
            sql="""
            select 
            a.id,
            a.from_theme_id,
            a.to_theme_id,
            a.process_mode,
            a.from_type,
            b.name as to_theme_name,
            c.config
            from (select * from ana_program where id='{}')a
            left join ana_theme b
            on a.to_theme_id=b.id
            left join ana_program_config c
            on a.id=c.program_id
            """.format(self.programId)
            self.cursor.execute(sql)
            rest=self.cursor.fetchone()
            self.from_theme_id=rest[1]
            self.to_theme_id=rest[2]
            self.from_type=rest[4]
            self.to_theme_name=rest[5]
            self.config=json.loads(rest[6])
            #------------------主表筛选检查
            if 'inputFilter' in self.config:
                theme_id = self.config['inputFilter']['themeId']
                theme_type = self.config['inputFilter']['themeType']
                self.df['input_filter_comment']=self.df.apply(lambda row:self.filter(row,'inputFilter',theme_id,theme_type),axis=1)
                self.df=self.df[self.df['input_filter_comment']==0]

            out_col_dataType={}
            if len(self.df)>0:
                #明确输出表字段
                label_map = self.config['label']['labelMap']
                # theme_filed_l ：关联表的字段(展示字段+排序字段)
                theme_filed_l=[]
                for value in label_map.values():
                    out_col_dataType[value['fieldName']]=value['dataType']
                    if value['ifGroup']=='是':
                        _d1={value['themeId']:value['fieldName']}
                        _d2={value['themeId']: value['sortFieldName']}
                        if _d1 not in theme_filed_l:
                            theme_filed_l.append(_d1)
                        if _d2 not in theme_filed_l:
                            theme_filed_l.append(_d2)
                theme_filed_d = {}
                for item in theme_filed_l:
                    for key, value in item.items():
                        if key in theme_filed_d:
                            theme_filed_d[key].append(value)
                        else:
                            theme_filed_d[key] = [value]
                #theme_filed_d输出内容格式：{'关联主题1id': ['关联主题1输出字段1', '关联主题1输出字段2',...], '关联主题2id': ['关联主题2输出字段1', '关联主题2输出字段2',...]}
                #------------------关联映射配置-关联配置
                if 'join' in self.config:
                    join_themes=self.config['join']['joinThemes']
                    left_theme_df = self.df
                    for ii in range(len(join_themes)):
                        join_theme=join_themes[ii]
                        on_list=join_theme['onList']
                        to_type=join_theme['toType']
                        to_themeid=join_theme['toThemeId']
                        left_on=[]
                        right_on=[]
                        old_right_on=[]
                        for on in on_list:
                            # 多表连接，上一次连接时会修改字段名称，所以》=2次连接时，连接键也要变化
                            if ii>=1 and (join_theme['fromThemeId']==join_themes[ii-1]['toThemeId']):
                                left_on_name=on['fromFieldName']+'_'+join_theme['fromThemeId']
                            else:
                                left_on_name=on['fromFieldName']
                            left_on.append(left_on_name)
                            old_right_on.append(on['toFieldName'])
                            right_on.append(on['toFieldName']+'_'+to_themeid)
                        right_theme_df =self.get_theme_data(to_themeid,to_type)
                        if len(right_theme_df):
                            for filed_name in theme_filed_d[join_theme['toThemeId']]:
                                right_theme_df.rename(columns={filed_name: filed_name+  '_'+join_theme['toThemeId']}, inplace=True)
                            for filed_name in old_right_on:
                                if filed_name in right_theme_df.columns:
                                    right_theme_df.rename(columns={filed_name: filed_name + '_' + join_theme['toThemeId']}, inplace=True)
                            if join_theme['logicType']=='and':
                                df_merge=pd.merge(left_theme_df,right_theme_df,how='inner',left_on=left_on,right_on=right_on)
                                left_theme_df=self.set_column_name(left_theme_df, right_theme_df, df_merge,left_on)
                            else:
                                df_merge_list=[]
                                for k in range(len(left_on)):
                                    left_on_or=left_on[k:k+1]
                                    right_on_or = right_on[k:k+1]
                                    df_merge=pd.merge(left_theme_df, right_theme_df, how='inner', left_on=left_on_or,right_on=right_on_or)
                                    df_merge = self.set_column_name(left_theme_df, right_theme_df, df_merge, left_on_or)
                                    df_merge_list.append(df_merge)
                                left_theme_df=pd.concat(df_merge_list)
                                left_theme_df=left_theme_df.drop_duplicates()
                        else:
                            left_theme_df=pd.DataFrame({})
                    self.df=left_theme_df


                if len(self.df)>0:
                    #------------------关联映射配置-标签组合
                    label_map=self.config['label']['labelMap']

                    group_dict={}
                    cal_dict={}

                    for value in label_map.values():
                        if value['ifGroup']=='否':
                            group_dict[value['fieldName'] ]= value['label']
                        else:
                            cal_dict[value['fieldName'] ]= value['label']
                    #增加排序规则列
                    for key,col in cal_dict.items():
                        rule=label_map[col]
                        filed_name_n =rule['fieldName']+'_sort'
                        self.df[filed_name_n]=self.df.apply(lambda x:rule,axis=1)
                    if len(self.df)>0:
                        #---------创建聚合计算列


                        group_cols=list(group_dict.keys())
                        # 处理聚合字段中的空值
                        for col in group_cols:
                            if self.df[col].dtype == 'object':
                                self.df[col].fillna('空值', inplace=True)
                            elif self.df[col].dtype == 'float64':
                                self.df[col].fillna(-9999, inplace=True)
                            elif self.df[col].dtype == 'datetime64[ns]':
                                self.df[col].fillna(datetime.datetime(1900, 1, 1, 1, 1, 1), inplace=True)

                        # 聚合列增加createTime
                        cal_dict['createTime'] = 'createTime'
                        cal_cols =list(cal_dict.keys())


                        if len(cal_cols)>0:
                            groupbylist = []
                            for cal_col in cal_cols:
                                cal_col1=cal_dict.get(cal_col)
                                df_group1 = self.df.groupby(group_cols).apply(lambda x: self.agg_cal(x, cal_col)).reset_index(name=cal_col1)
                                groupbylist.append(df_group1)
                            self.df = groupbylist[0]
                            for i in range(1, len(groupbylist)):
                                self.df = pd.merge(self.df, groupbylist[i], how='inner', on=group_cols)
                        else:
                            self.df = self.df[group_cols].drop_duplicates()
                        # 还原空值
                        for col in group_cols:
                            if self.df[col].dtype=='object':
                                self.df[col].replace('空值',np.nan,inplace=True)
                            elif self.df[col].dtype=='float64':
                                self.df[col].replace(-9999, np.nan,inplace=True)
                            elif self.df[col].dtype == 'datetime64[ns]':
                                self.df[col].replace(datetime.datetime(1,1,1,1,1,1,1), np.nan, inplace=True)

                        #group_dict.update(cal_dict)
                        self.df.rename(columns=group_dict,inplace=True)
                        # 删除辅助列
                        drop_columns=list(self.df.columns)
                        reserve_columns=['createTime']
                        drop_columns=[i for i in drop_columns if i not in reserve_columns]
                        if len(self.df) > 0:
                            #----------------------------关联映射配置-映射配置

                            mapping_list = self.config['mapping']['mappingList']

                            const_mapping_cols={}
                            null_cols=[]
                            formula_dict={}
                            for mapping in mapping_list:
                                if mapping['type']=='直接映射':
                                    #直接映射
                                    self.df[mapping['fieldName']]=self.df[mapping['value']]
                                elif mapping['type']=='固定常量':
                                    const_mapping_cols[mapping['fieldName']]=mapping['value']
                                elif mapping['type']=='设置为空':
                                    null_cols.append(mapping['fieldName'])
                                elif mapping['type']=='公式计算':
                                    formula_dict[mapping['fieldName']]=mapping['value']

                            # 公式计算
                            for key, value in formula_dict.items():
                                self.df[key] = self.df.apply(lambda x: self.calculate(x, value), axis=1)

                            self.df.drop(columns=drop_columns,inplace=True)
                            # 固定常量
                            for key, value in const_mapping_cols.items():
                                self.df[key] = value
                            # 设置为空
                            for null_col in null_cols:
                                self.df[null_col] = np.nan

                            # ------------------输出表筛选条件
                            if 'outputFilter' in self.config:
                                theme_id=self.config['outputFilter']['themeId']
                                theme_type='业务主题'
                                self.df['output_filter_comment'] = self.df.apply(lambda row: self.filter(row, 'outputFilter',theme_id,theme_type), axis=1)
                                self.df = self.df[self.df['output_filter_comment'] == 0]
                                self.df=self.df.drop(columns=['output_filter_comment'])
                            if len(self.df) > 0:
                                # 调整createTime列为最后一列
                                create_time=self.df['createTime']
                                self.df=self.df.drop('createTime', axis=1)
                                self.df.insert(self.df.shape[1],'createTime',create_time)
        self.add_lose_cols(self.to_theme_id)
        return self.df






