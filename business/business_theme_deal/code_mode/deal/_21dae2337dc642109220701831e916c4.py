import pymongo
import json
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
        create_time=str(self.data.get("createTime"))
        json_data = json.loads(json_str)
        result_list = []
        if "Licens" in json_data:
            for item in json_data["Licens"]:
                result_map = {}
                result_map["企业名称"] = json_data.get("Name")
                result_map["企业ID"] = json_data.get("Unique")
                result_map["许可文件编号"] = item.get("LicensDocNo")
                result_map["许可文件名称"] = item.get("LicensDocName")
                result_map["有效期自"] = item.get("ValidityFrom")
                result_map["有效期至"] = item.get("ValidityTo")
                result_map["许可机关"] = item.get("LicensOffice")
                result_map["许可内容"] = item.get("LicensContent")
                result_map["createTime"] = create_time
                result_list.append(result_map)
        return result_list
# conn = pymongo.MongoClient(inputserver)['Inside-Data-Basic-Out']
# collection=conn['10049a9724f84cddb8245b13a8a80fdf']
# result=[]
# for data in collection.find({}).limit(10):
#     result=result+CodeMode(data, 'Inside-Data-Basic-Out','Inside-Data-Business-Out').process()
# print(result)