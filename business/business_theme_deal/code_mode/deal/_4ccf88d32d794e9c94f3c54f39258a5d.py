import datetime
import json
import pymongo
from public.config import *
import re
from decimal import Decimal


class CodeMode:
    def __init__(self,data,themeDatabase, businessDatabase):
        self.data=data
        self.themeDatabase=themeDatabase
        self.businessDatabase=businessDatabase
    def get_mongo_data(self,database,collection_name):
        conn = pymongo.MongoClient(inputserver)[database]
        collection=conn[collection_name]
        return collection
    def format_number_djqx(self, number):
        if number is None or number == "":
            return ""
        else:
            return number.replace('天','')

    def format_number_equitymount(self, number):
        if number is None or number == "":
            return None
        else:
            match = re.search(r'([\d,.]+)', number)  # 匹配任意数字、逗号和小数点
            if match:
                number_str = match.group(1)
                number = Decimal(number_str.replace(',', '')) * Decimal('10000')  # 将数字中的逗号替换为空格，并转换为浮点数
                return number
            return None

    def format_date(self, date):
        if date is None:
            return ""
        else:
            try:
                date_object = datetime.datetime.strptime(date, "%Y-%m-%d")
                formatted_date = date_object.strftime("%Y-%m-%d")
                return formatted_date
            except ValueError:
                return ""

    def get_maps(self, json_data,create_time):
        data = json.loads(json_data)
        lst = []
        if "Assistance" not in data:
            return lst

        for item in data["Assistance"]:
            map = {}
            map["企业名称"] = data["Name"]
            map["企业ID"] = data["Unique"]
            jsonObject1 = item

            map["执行文书文号"] = jsonObject1.get("ExecutionNoticeNum", "")
            map["被执行人"] = jsonObject1.get("ExecutedBy", "")
            map["被执行人ID"] = jsonObject1.get("KeyNo", "")

            relatedCompanyInfo = jsonObject1.get("RelatedCompanyInfo")
            if relatedCompanyInfo is None:
                map["冻结股权标的企业"] = ""
            else:
                map["冻结股权标的企业"] = relatedCompanyInfo.get("Name", "")

            equityFreezeDetail = jsonObject1.get("EquityFreezeDetail")
            if equityFreezeDetail is None:
                map["执行事项"] = ""
                map["执行裁定书文号"] = ""
                map["被执行人证件种类"] = ""
                map["被执行人证件号码"] = ""
                map["冻结期限自"] = ""
                map["冻结期限至"] = ""
                map["冻结期限"] = ""
                map["公示日期"] = ""
            else:
                map["执行事项"] = equityFreezeDetail.get("ExecutionMatters", "")
                map["执行裁定书文号"] = equityFreezeDetail.get("ExecutionVerdictNum", "")
                map["被执行人证件种类"] = equityFreezeDetail.get("ExecutedPersonDocType", "")
                map["被执行人证件号码"] = equityFreezeDetail.get("ExecutedPersonDocNum", "")
                map["冻结期限自"] = self.format_date(equityFreezeDetail.get("FreezeStartDate"))
                map["冻结期限至"] = self.format_date(equityFreezeDetail.get("FreezeEndDate"))
                map["冻结期限"] = self.format_number_djqx(equityFreezeDetail.get("FreezeTerm", ""))
                map["公示日期"] = self.format_date(equityFreezeDetail.get("PublicDate"))

            map["执行法院"] = jsonObject1.get("EnforcementCourt", "")
            map["执行通知书文号"] = jsonObject1.get("ExecutionNoticeNum", "")
            map["执行人持有股权、其他投资权益的数额（元）"] = self.format_number_equitymount(jsonObject1.get("EquityAmount", ""))
            map["状态"] = jsonObject1.get("StatuesDetail", "")
            map['createTime']=create_time
            lst.append(map)
        return lst
    def process(self):
        json_data = self.data.get("json")
        create_time = str(self.data.get("createTime"))
        lst = self.get_maps(json_data,create_time)
        return lst
