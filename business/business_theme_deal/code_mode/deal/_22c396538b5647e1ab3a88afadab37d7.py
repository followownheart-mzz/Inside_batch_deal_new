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
        list.append(self.data)
        return list
# conn = pymongo.MongoClient(inputserver)['Inside-Data-Basic-Out']
# collection=conn['10049a9724f84cddb8245b13a8a80fdf']
# for data in collection.find({}):
#     print(CodeMode(data, 'Inside-Data-Basic-Out','Inside-Data-Business-Out').process())