import json
import pymongo
from public.config import *


class CodeMode:
    def __init__(self,data,themeDatabase, businessDatabase):
        self.data=data
        self.themeDatabase=themeDatabase
        self.businessDatabase=businessDatabase
    def get_mongo_data(self,database,collection_name):
        conn = pymongo.MongoClient(inputserver)[database]
        collection=conn[collection_name]
        return collection
    def process(self):
        json_str = str(self.data.get("json"))
        jsonObject = json.loads(json_str)
        create_time=self.data.get('createTime')
        list = []
        if "Partners" in jsonObject:
            for object in jsonObject["Partners"]:
                jsonObject1 = object
                map = {}
                map["企业名称"] = jsonObject.get("Name")
                map["企业ID"] = jsonObject.get("Unique")

                map["人员名称"] = jsonObject1.get("StockName")
                map["人员ID"] = jsonObject1.get("KeyNo")
                map["职位"] = jsonObject1.get("Job")

                tags = jsonObject1.get("Tags")

                if tags is None:
                    map["有失信"] = None
                    map["有限制高消费"] = None
                    map["有被执行"] = None
                else:
                    map["有失信"] = "是" if "有失信" in tags else "否"
                    map["有限制高消费"] = "是" if "有限制高消费" in tags else "否"
                    map["有被执行"] = "是" if "有被执行" in tags else "否"
                map['createTime']=create_time
                list.append(map)
        return list
