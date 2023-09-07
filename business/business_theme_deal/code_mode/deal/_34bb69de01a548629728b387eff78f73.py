import pymongo
from public.config import *


class CodeMode:
    def __init__(self,data,themeDatabase, businessDatabase):
        self.data=data.to_dict()
        self.themeDatabase=themeDatabase
        self.businessDatabase=businessDatabase
        self.collection = None
    def get_mongo_data(self,database,collection_name):
        conn = pymongo.MongoClient(inputserver)[database]
        collection=conn[collection_name]
        return collection
    def process(self):
        list_=[]
        srfszcs = self.data.get("受让方名称-所在城市")
        dkssqszcs = self.data.get("地块所属区（市）-所在城市")
        if srfszcs is None or dkssqszcs is None or srfszcs == dkssqszcs or srfszcs == "" or dkssqszcs == "":
            return list_
        if srfszcs == "市辖区" or dkssqszcs == "市辖区":
            srfszsf = self.data.get("受让方名称-所在省份")
            dkssqszsf = self.data.get("地块所属区（市）-所在省份")
            if srfszsf == dkssqszsf:
                return list_
        list_.append(self.data)
        return list_
# conn = pymongo.MongoClient(inputserver)['Inside-Data-Business-Out']
# collection=conn['01c3096eb2794f3f9c0ad7bf58f205f8']
# result=[]
# for data in collection.find({}).limit(100):
#     result=result+CodeMode(data, 'Inside-Data-Basic-Out','Inside-Data-Business-Out').process()
# print(result)