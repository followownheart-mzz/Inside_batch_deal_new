import pymongo
from public.config import *


class CodeMode:
    def __init__(self,data,themeDatabase, businessDatabase):
        self.data=data.to_dict()
        self.themeDatabase=themeDatabase
        self.businessDatabase=businessDatabase
        self.collection = None
    def get_mongo_data(self,database,collection_name):
        conn = pymongo.MongoClient(inputserver)[database]
        collection=conn[collection_name]
        return collection
    def process(self):
        list = []
        datas = {key: self.data[key] for key in ["项目名称","省级项目编号","建设单位","项目分类","项目属地","立项文号","立项级别","总投资（元）","总面积（平方米）",
                                            "建设性质","工程用途","省级竣工备案编号","实际造价（元）","实际面积（平方米）","实际建设规模","结构体系","实际开工日期",
                                            "实际竣工日期","设计单位名称","监理单位名称","施工单位名称","createTime"]}
        filter1 = True
        jybd_city = self.data.get("项目属地-所在城市")
        mf_city = self.data.get("施工单位名称-所在城市")
        if jybd_city is None or mf_city is None or jybd_city == mf_city or jybd_city == "" or mf_city == "":
            filter1 = False
        else:
            if jybd_city == "市辖区" or mf_city == "市辖区":
                jybd_province = self.data.get("项目属地-所在省份")
                mf_province = self.data.get("施工单位名称-所在省份")
                if jybd_province == mf_province:
                    filter1 = False

        filter2 = True
        maif_city = self.data.get("建设单位-所在城市")
        if jybd_city is None or maif_city is None or jybd_city == maif_city or jybd_city == "" or maif_city == "":
            filter2 = False
        else:
            if jybd_city == "市辖区" or maif_city == "市辖区":
                jybd_province = self.data.get("项目属地-所在省份")
                maif_province = self.data.get("建设单位-所在省份")
                if jybd_province == maif_province:
                    filter2 = False

        if filter1 or filter2:
            list.append(datas)

        return list
