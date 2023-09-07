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
        otherObject = (
            json.loads(other)
            .get("allResultList")[0]
            .get("json")
            .get("YYZZXX")
        )
        list = []
        if "Penalty" in jsonObject:
            for penalty in jsonObject.get("Penalty"):
                map = {}
                map["企业名称"] = jsonObject.get("Name")
                map["企业ID"] = jsonObject.get("Unique")
                map["决定文书号"] = penalty.get("DocNo")
                map["违法行为类型"] = penalty.get("PenaltyType")
                map["行政处罚内容"] = penalty.get("Content")
                map["处罚决定日期"] = penalty.get("PenaltyDate")
                map["公示日期"] = penalty.get("PublicDate")
                map["决定机关"] = penalty.get("OfficeName")
                map['createTime']=create_time
                list.append(map)
        return list
# conn = pymongo.MongoClient(inputserver)['Inside-Data-Basic-Out']
# collection=conn['10049a9724f84cddb8245b13a8a80fdf']
# for data in collection.find({}).limit(100):
#     print(CodeMode(data, 'Inside-Data-Basic-Out','Inside-Data-Business-Out').process())