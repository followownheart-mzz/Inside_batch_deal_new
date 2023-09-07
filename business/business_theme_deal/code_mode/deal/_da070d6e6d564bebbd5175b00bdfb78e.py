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
        jsonObject = json.loads(json_str)
        otherObject = (
            json.loads(other)
            .get("allResultList")[0]
            .get("json")
            .get("YYZZXX")
        )
        list = []
        if "Partners" in jsonObject:
            for partner in jsonObject.get("Partners"):
                map = {}
                map["企业名称"] = jsonObject.get("Name")
                map["股东名称"] = partner.get("StockName")
                map["出资时间"] = partner.get("ShoudDate")
                map["认缴出资"] = partner.get("ShouldCapi")
                map["实缴出资"] = partner.get("RealCapi")
                map["股东性质"] = partner.get("StockType")
                map["股东ID"] = partner.get("KeyNo")
                map["出资比例（%）"] = partner.get("StockPercent")
                tags = partner.get("Tags")
                if tags is None:
                    map["大股东"] = None
                    map["实际控制人"] = None
                    map["最终受益人"] = None
                else:
                    map["大股东"] = "是" if "大股东" in tags else "否"
                    map["实际控制人"] = "是" if "实际控制人" in tags else "否"
                    map["最终受益人"] = "是" if "最终受益人" in tags else "否"
                map["标签"] = None
                map["采集时间"] = otherObject.get("采集时间")
                map['createTime']=self.data.get('createTime')
                list.append(map)
        return list
# conn = pymongo.MongoClient(inputserver)['Inside-Data-Basic-Out']
# collection=conn['10049a9724f84cddb8245b13a8a80fdf']
# for data in collection.find({}).limit(100):
#     print(CodeMode(data, 'Inside-Data-Basic-Out','Inside-Data-Business-Out').process())