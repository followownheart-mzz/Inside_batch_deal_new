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
        list = []
        collection=self.get_mongo_data(self.themeDatabase, "462d9dfe60a696aaa631b8073dc1b894")
        xmmc = self.data.get("项目名称")
        fs = self.data.get("采购方式")
        jg = self.data.get("代理机构")
        cgr = self.data.get("采购人（甲方）")
        htje = self.data.get("合同金额（元）")
        htqdrq = self.data.get("合同签订日期")
        gys = self.data.get("供应商（乙方）")
        gsbt = self.data.get("公示标题")
        cgfs = ""
        dljg = ""
        createTime=self.data.get('createTime')

        datas = {}
        datas["项目名称"] = xmmc
        datas["招标方名称"] = cgr
        datas["中标金额（元）"] = htje
        datas["招标方式"] = fs
        datas["中标时间"] = htqdrq
        datas["中标单位名称"] = gys
        datas["集中采购机构名称"] = jg
        datas["公示标题"] = gsbt
        datas["createTime"] = createTime
        if isinstance(fs, str) and isinstance(jg, str):
            cgfs = str(fs)
            dljg = str(jg)
        else:
            return list

        if cgfs == "" and dljg != "":
            document1 = {"采购项目名称": xmmc}
            document = collection.find_one(document1)
            if document is None:
                list.append(datas)
                return list
            o = document.get("采购方式")
            datas["招标方式"] = o
        elif cgfs != "" and dljg == "":
            document1 = {"采购项目名称": xmmc}
            document = collection.find_one(document1)
            if document is None:
                return list
            o = document.get("代理机构名称")
            datas["集中采购机构名称"] = o
        elif cgfs == "" and dljg == "":
            document1 = {"采购项目名称": xmmc}
            document = collection.find_one(document1)
            if document is None:
                return list
            o = document.get("采购方式")
            o1 = document.get("代理机构名称")
            datas["招标方式"] = o
            datas["集中采购机构名称"] = o1

        string = str(datas.get("集中采购机构名称"))
        if string == "":
            return list

        list.append(datas)
        return list
# conn = pymongo.MongoClient(inputserver)['Inside-Data-Basic-Out']
# collection=conn['3b9a735be41228add506f656f603b26b']
# for data in collection.find({}):
#     print(CodeMode(data, 'Inside-Data-Basic-Out','Inside-Data-Business-Out').process())