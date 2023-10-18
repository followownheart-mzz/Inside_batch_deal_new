import datetime
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
        if self.collection is None:
            self.collection = self.get_mongo_data(self.themeDatabase,"fc3c236ce47e4ad89169870383c0ae47")
        query = {}
        query["商品房名称"] = self.data.get("商品房名称")
        query["楼层"] = self.data.get("楼层")
        query["门牌号"] = self.data.get("门牌号")
        query["单元号"] = self.data.get("单元号")
        query["楼栋号"] = self.data.get("楼栋号")
        query["房地产公司名称"] = self.data.get("房地产公司名称")
        query["总价（元）"] = self.data.get("总价（元）")
        query["总建筑面积（平方米）"] = self.data.get("总建筑面积（平方米）")
        query["房屋用途"] = self.data.get("房屋用途")
        query["套内建筑面积（平方米）"] = self.data.get("套内建筑面积（平方米）")
        query["商品房坐落"] = self.data.get("商品房坐落")

        first =self.collection.find_one(query)
        if first is None:
            return list

        zt = str(self.data.get("房屋状态"))
        xkzt = str(first.get("房屋状态"))
        if zt == "已售" and xkzt != "已售":
            map = {}
            map["商品房名称"] = first.get("商品房名称")
            map["楼层"] = first.get("楼层")
            map["门牌号"] = first.get("门牌号")
            map["单元号"] = first.get("单元号")
            map["楼栋号"] = first.get("楼栋号")
            map["房地产公司名称"] = first.get("房地产公司名称")
            map["销售时间"] = datetime.datetime.now().strftime('%Y-%m-%d')
            map["总价（元）"] = first.get("总价（元）")
            map["总建筑面积（平方米）"] = first.get("总建筑面积（平方米）")
            map["所属区域"] = first.get("所属区域")
            map["房屋用途"] = first.get("房屋用途")
            map["套内建筑面积（平方米）"] = first.get("套内建筑面积（平方米）")
            map["分摊建筑面积（平方米）"] = first.get("分摊建筑面积（平方米）")
            map["房屋状态"]= "已售"
            map["商品房坐落"] = first.get("商品房坐落")
            map['createTime']=self.data.get('createTime')
            list.append(map)
            return list
        return list
