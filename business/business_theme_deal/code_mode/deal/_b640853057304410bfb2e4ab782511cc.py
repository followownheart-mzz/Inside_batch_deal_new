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
        other = str(self.data.get("other"))
        create_time=self.data.get('createTime')
        jsonObject = json.loads(json_str)
        otherObject = json.loads(other)["allResultList"][0]["json"]["YYZZXX"]
        list = []
        map = {}
        companyExtendInfo=jsonObject.get("companyExtendInfo")
        if  companyExtendInfo:
            desc = companyExtendInfo.get("Desc")
            if desc is not None and len(desc) > 0:
                map["企业名称"] = jsonObject["Name"]
                map["企业ID"] = jsonObject["Unique"]
                map["企业简介"] = desc
                map['createTime']=create_time
                list.append(map)
        return list
