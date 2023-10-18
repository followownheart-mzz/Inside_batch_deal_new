import pymongo
from public.config import *


class CodeMode:
    def __init__(self,data,themeDatabase, businessDatabase):
        self.data=data.to_dict()
        self.themeDatabase=themeDatabase
        self.businessDatabase=businessDatabase
        self.collection=None
    def get_mongo_data(self,database,collection_name):
        conn = pymongo.MongoClient(inputserver)[database]
        collection=conn[collection_name]
        return collection
    def process(self):
        list = []
        col_list = ["披露日期","最新公告日","最新进度","交易标的","交易金额","币种","买方","卖方","支付方式","标的类型","并购方式","与披露方关系","控制权是否变更",
"交易对披露方影响","交易简介","详细情况","并购目的","是否关联交易","买方证券代码","买方性质","买方与披露方关系","受让后持股数量（股）","受让后持股比例（%）","卖方证券代码","卖方性质",
"卖方与披露方关系","股权转让比例（%）","转让后持股数量（股）","转让前持股比例（%）","转让后持股比例（%）","股票代码","披露方","createTime"]
        datas = {key: value for key, value in self.data.items() if key in col_list}
        filter1 = True
        jybd_city = self.data.get("交易标的-所在城市")
        mf_city = self.data.get("买方-所在城市")
        if (
                jybd_city is None
                or mf_city is None
                or jybd_city == mf_city
                or jybd_city == ""
                or mf_city == ""
        ):
            filter1 = False
        else:
            if jybd_city == "市辖区" or mf_city == "市辖区":
                jybd_province = self.data.get("交易标的-所在省份")
                mf_province = self.data.get("买方-所在省份")
                if jybd_province == mf_province:
                    filter1 = False

        filter2 = True
        maif_city = self.data.get("卖方-所在城市")
        if (
                jybd_city is None
                or maif_city is None
                or jybd_city == maif_city
                or jybd_city == ""
                or maif_city == ""
        ):
            filter2 = False
        else:
            if jybd_city == "市辖区" or maif_city == "市辖区":
                jybd_province = self.data.get("交易标的-所在省份")
                maif_province = self.data.get("卖方-所在省份")
                if jybd_province == maif_province:
                    filter2 = False

        if filter1 or filter2:
            list.append(datas)

        return list
