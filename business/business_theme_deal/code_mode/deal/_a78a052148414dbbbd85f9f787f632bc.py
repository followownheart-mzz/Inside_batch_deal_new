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
        srfszcs = self.data.get("受让方名称-所在城市")
        dkssqszcs = self.data.get("地块所属区（市）-所在城市")
        if srfszcs is None or dkssqszcs is None or srfszcs == dkssqszcs or srfszcs == "" or dkssqszcs == "":
            return list

        if srfszcs == "市辖区" or dkssqszcs == "市辖区":
            srfszsf = self.data.get("受让方名称-所在省份")
            dkssqszsf = self.data.get("地块所属区（市）-所在省份")

            if srfszsf == dkssqszsf:
                return list
        list.append(self.data)
        return list
# conn = pymongo.MongoClient(inputserver)['Inside-Data-Business-Out']
# collection=conn['61d78f2db21547f49ab4b92ebbd058b8']
# for data in collection.find({}):
#     print(CodeMode(data, 'Inside-Data-Basic-Out','Inside-Data-Business-Out').process())