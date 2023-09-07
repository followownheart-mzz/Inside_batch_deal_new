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
    def remove_redundant_strings(self,_list):
        return [s.replace('<em>','').replace('</em>','') for s in _list]
    def process(self):
        json_str = str(self.data.get("json"))
        other = str(self.data.get("other"))
        create_time=self.data.get("createTime")
        jsonObject = json.loads(json_str)
        otherObject = json.loads(other)["allResultList"][0]["json"]["YYZZXX"]
        list_ = []
        if "ChangeDiffInfo" in jsonObject:
            jsonChange = jsonObject["ChangeDiffInfo"]
            if "ChangeList" in jsonChange:
                for obj in jsonChange["ChangeList"]:
                    jsonObject1 = obj
                    map_ = {}
                    map_["企业名称"] = jsonObject.get("Name")
                    map_["变更事项"] = jsonObject1.get("ProjectName")
                    map_["变更前内容"] = str(self.remove_redundant_strings(jsonObject1.get("BeforeList", "")))
                    map_["变更后内容"] = str(self.remove_redundant_strings(jsonObject1.get("AfterList", "")))
                    map_["变更日期"] = jsonObject1.get("ChangeDate")
                    map_["采集时间"] = otherObject.get("采集时间")
                    map_["createTime"]=create_time
                    list_.append(map_)

        return list_
# conn = pymongo.MongoClient(inputserver)['Inside-Data-Basic-Out']
# collection=conn['10049a9724f84cddb8245b13a8a80fdf']
# for data in collection.find({}).limit(20):
#     print(CodeMode(data, 'Inside-Data-Basic-Out','Inside-Data-Business-Out').process())