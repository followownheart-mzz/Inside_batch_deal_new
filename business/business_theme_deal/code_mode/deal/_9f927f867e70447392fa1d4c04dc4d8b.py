import pymongo
from public.config import *
import datetime
class CodeMode:
    def __init__(self,data,themeDatabase, businessDatabase):
        self.data=data.to_dict()
        self.themeDatabase=themeDatabase
        self.businessDatabase=businessDatabase
        self.collection = None
        self.a=0
    def get_mongo_data(self,database,collection_name):
        conn = pymongo.MongoClient(inputserver)[database]
        collection=conn[collection_name]
        return collection
    def process(self):
        list = []
        # 基础主题 西安市住房和城乡建设局-房屋销控 id
        if self.collection is None:
            self.collection = self.get_mongo_data(self.themeDatabase,"7efb5f6efe304a639a0af65efd801817")
        query = {
            "项目名称": self.data.get("项目名称"),
            "所在层": self.data.get("所在层"),
            "房间号": self.data.get("房间号"),
            "所在单元": self.data.get("所在单元"),
            "幢号": self.data.get("幢号"),
            "开发企业": self.data.get("开发企业"),
            "房屋用途": self.data.get("房屋用途"),
            "项目坐落": self.data.get("项目坐落")
        }
        first = self.collection.find_one(query)
        if first is None:
            return list
        zt = str(self.data.get("状态")) #西安市住房和城乡建设局-房屋状态
        xkzt = str(first.get("状态")) #西安市住房和城乡建设局-房屋销控
        if zt == "已网签备案" and xkzt != "已网签备案":
            map = {
                "商品房名称": first.get("项目名称"),
                "楼层": first.get("所在层"),
                "门牌号": first.get("房间号"),
                "单元号": first.get("所在单元"),
                "楼栋号": first.get("幢号"),
                "房地产公司名称": first.get("开发企业"),
                "销售时间": datetime.datetime.now().strftime('%Y-%m-%d'),
                "总建筑面积（平方米）": first.get("建筑面积（平方米）"),
                "所属区域": first.get("所属区域"),
                "房屋用途": first.get("房屋用途"),
                "套内建筑面积（平方米）": first.get("套内面积（平方米）"),
                "分摊建筑面积（平方米）": first.get("分摊面积（平方米）"),
                "房屋状态": "已网签备案",
                "商品房坐落": first.get("项目坐落"),
                "总价（元）":None,
                "createTime":self.data.get('createTime')
            }
            list.append(map)
        return list
