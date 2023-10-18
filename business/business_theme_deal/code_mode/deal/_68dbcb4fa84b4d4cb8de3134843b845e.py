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
        if "Branches" not in jsonObject:
            return list
        for obj in jsonObject["Branches"]:
            jsonObject1 = obj
            map = {}
            map["企业名称"] = jsonObject.get("Name")
            map["企业ID"] = jsonObject.get("Unique")
            map["机构名称"] = jsonObject1.get("Name")
            map["机构ID"] = jsonObject1.get("KeyNo")

            oper = jsonObject1.get("Oper")

            map["法定代表人"] = oper.get("Name") if oper else ""
            map["法定代表人ID"] = oper.get("KeyNo") if oper else ""
            map["登记机关"] = jsonObject1.get("BelongOrg")
            map["经营状态"] = jsonObject1.get("ShortStatus")
            map['createTime']=create_time
            list.append(map)
        return list
