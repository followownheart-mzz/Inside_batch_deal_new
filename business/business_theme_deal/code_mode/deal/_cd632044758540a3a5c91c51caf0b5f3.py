import json
import pymongo
import re
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
    def format_number(self,s):
        if s:
            if '万元人民币' in s:
                result = re.sub('万元人民币', '', s)
                result="{:,.2f}".format(float(result)*10000)
                return result
        return None
    def process(self):
        json_str = str(self.data.get("json"))
        other_str = str(self.data.get("other"))
        jsonObject = json.loads(json_str)
        otherObject = (
            json.loads(other_str)
            .get("allResultList")[0]
            .get("json")
            .get("YYZZXX")
        )
        list = []
        map = {}
        map["企业名称"] = jsonObject.get("Name")

        originalName = jsonObject.get("OriginalName")
        if originalName is None:
            map["企业曾用名"] = ""
        else:
            _set = set()
            for obj in originalName:
                _set.add(obj.get("Name"))
            if len(_set) == 0:
                map["企业曾用名"] = ""
            else:
                join = ",".join(_set)
                map["企业曾用名"] = join

        map["核准日期"] = jsonObject.get("CheckDate")
        map["企业机构类型"] = jsonObject.get("EconKind")
        map["统一社会信用代码"] = jsonObject.get("CreditCode")
        map["注册地"] = jsonObject.get("Address")
        map["企业联系电话"] = (
            jsonObject.get("ContactInfo").get("PhoneNumber")
            if jsonObject.get("ContactInfo") is not None
            else ""
        )
        map["注册时间"] = jsonObject.get("StartDate")
        map["登记机关"] = jsonObject.get("BelongOrg")
        map["组织机构代码"] = jsonObject.get("OrgNo")
        map["法定代表人"] = jsonObject.get("Oper").get("Name")
        map["注册资本"] = jsonObject.get("RegistCapi")
        s = jsonObject.get("RegistCapi")
        map["注册资本（元）"] = self.format_number(s)

        industry = jsonObject.get("Industry")
        map["所属行业"] = industry.get("Industry") if industry is not None else ""
        map["行业划分"] = industry.get("SubIndustry") if industry is not None else ""
        map["经营范围"] = jsonObject.get("Scope")
        map["经营状态"] = jsonObject.get("ShortStatus")
        map["参保人数"] = ""
        map["实缴资本"] = jsonObject.get("RecCap")

        companyExtendInfo = jsonObject.get("companyExtendInfo")

        map["人员规模"] = (
            companyExtendInfo.get("Info") if companyExtendInfo is not None else ""
        )
        map["纳税人识别号"] = jsonObject.get("TaxNo")
        map["工商注册号"] = jsonObject.get("No")
        map["进出口企业代码"] = jsonObject.get("IxCode")
        map["英文名"] = jsonObject.get("EnglishName")
        map["经营方式"] = (
            companyExtendInfo.get("Industry")
            if companyExtendInfo is not None
            else ""
        )
        termStart = jsonObject.get("TermStart")
        teamEnd = jsonObject.get("TeamEnd")
        if termStart:
            termStart += "-"
            if teamEnd:
                termStart += teamEnd
            else:
                termStart += '无固定期限'
        map["营业期限"] = termStart if termStart else ''

        map["网站"] = (
            companyExtendInfo.get("WebSite") if companyExtendInfo is not None else ""
        )
        map["邮箱"] = (
            companyExtendInfo.get("Email") if companyExtendInfo is not None else ""
        )
        # map["公司简介"] = (
        #     companyExtendInfo.get("Desc") if companyExtendInfo is not None else ""
        # )
        map["图片URL"] = jsonObject.get("ImageUrl")
        map["采集时间"] = otherObject.get("采集时间")

        area = jsonObject.get("Area")

        map["所在省份"] = area.get("Province") if area is not None else ""
        map["所在市"] = area.get("City") if area is not None else ""
        map["所在区县"] = area.get("County") if area is not None else ""
        map["qcc_id"] = jsonObject.get("Unique")
        if jsonObject.get("CommonList") is not None:
            for obj in jsonObject.get("CommonList"):
                if obj.get("KeyDesc") == "公司简称":
                    map["公司简称"] = obj.get("Value")
        map['createTime']=self.data.get('createTime')
        list.append(map)
        return list
