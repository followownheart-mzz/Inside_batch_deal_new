import json
import pymongo
from public.config import *


class CodeMode:
    def __init__(self,data,themeDatabase, businessDatabase):
        self.data=data.to_dict()
        self.themeDatabase=themeDatabase
        self.businessDatabase=businessDatabase
    def get_mongo_data(self,database,collection_name):
        conn = pymongo.MongoClient(inputserver)[database]
        collection=conn[collection_name]
        return collection
    def process(self):
        json_str = str(self.data.get("json"))
        other_str = str(self.data.get("other"))
        create_time=self.data.get('createTime')
        jsonObject = json.loads(json_str)
        otherObject = json.loads(other_str)["allResultList"][0]["json"]["YYZZXX"]
        list_ = []
        if "Employees" in jsonObject:
            for obj in jsonObject["Employees"]:
                jsonObject1 = obj
                map_ = {}
                map_["企业名称"] = jsonObject.get("Name")
                map_["高管名称"] = jsonObject1.get("Name")
                map_["高管职位"] = jsonObject1.get("Job")
                map_["高管ID"] = jsonObject1.get("KeyNo")
                map_["采集时间"] = otherObject.get("采集时间")
                map_["最终受益股份（%）"] =None
                map_['createTime']=create_time

                list_.append(map_)

        return list_
