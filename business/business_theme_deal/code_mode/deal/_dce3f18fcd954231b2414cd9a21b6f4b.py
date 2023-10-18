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
        if "Pledge" not in jsonObject:
            return list
        for pledge in jsonObject.get("Pledge"):
            map = {}
            map["企业名称"] = jsonObject.get("Name")
            map["企业ID"] = jsonObject.get("Unique")
            s = pledge.get("PledgedAmount")
            map["出质股权数额（元）"] = float(s)*10000
            map["登记编号"] = pledge.get("RegistNo")
            map["状态"] = pledge.get("Status")
            pledgorInfo = pledge.get("PledgorInfo")
            if pledgorInfo is None:
                map["出质人"] = ""
                map["出质人ID"] = ""
                map["出质人证件号码"] = ""
            else:
                map["出质人"] = pledgorInfo.get("Name")
                map["出质人ID"] = pledgorInfo.get("KeyNo")
                map["出质人证件号码"] = pledgorInfo.get("No")
            pledgeeInfo = pledge.get("PledgeeInfo")
            if pledgeeInfo is None:
                map["质权人"] = ""
                map["质权人ID"] = ""
                map["质权人证件号码"] = ""
            else:
                map["质权人"] = pledgeeInfo.get("Name")
                map["质权人ID"] = pledgeeInfo.get("KeyNo")
                map["质权人证件号码"] = pledgeeInfo.get("No")
            relatedCompanyInfo = pledge.get("RelatedCompanyInfo")
            map["股权出质登记日期"] = pledge.get("RegDate")
            map["出质股权标的企业"] = (
                "" if relatedCompanyInfo is None else relatedCompanyInfo.get("Name")
            )
            map['createTime']=self.data.get('createTime')
            list.append(map)
        return list
