# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import codecs
import json
import pymongo
import pickle
import os

from scrapy.exceptions import DropItem
from scrapy.exporters import CsvItemExporter, JsonItemExporter


class MovieScrapPipeline(object):
    def process_item(self, item, spider):
        return item


class DuplicatesPipeline(object):

    def __init__(self):
        self.ids_seen = set()
        if os.path.exists('ids_seen.pkl'):
            with open('ids_seen.pkl', 'rb') as f:
                self.ids_seen = pickle.load(f)
        print("아이디 전체갯수 : %s"%len(self.ids_seen))

    def close_spider(self, spider):
        with open('ids_seen.pkl', 'wb') as f:
            pickle.dump(self.ids_seen, f)

    def process_item(self, item, spider):
        if item['review_id'] in self.ids_seen:
            raise DropItem("Duplicate item found: %s" % item)
        else:
            self.ids_seen.add(item['review_id'])
            return item

class CommunityPipeline(object):

    words_to_filter = [u'19금', u'후방주의', u'오바마']

    def process_item(self, item, spider):
        for word in self.words_to_filter:
            if word in item['title']:
                raise DropItem("Contains forbidden word: %s" % word)
        else:
            return item

#CSV 파일로 저장하는 클래스
class CsvPipeline(object):
    def __init__(self):
        self.file = open("items.csv", 'wb')
        self.exporter = CsvItemExporter(self.file, encoding='utf-8')
        self.exporter.start_exporting()

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

#JSON파일로 저장하는 클래스
class JsonPipeline(object):
    def __init__(self):
        self.file = open("items.json", 'wb')
        self.exporter = JsonItemExporter(self.file, encoding='utf-8', ensure_ascii=False)
        self.exporter.start_exporting()

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

class JsonWriterPipeline(object):

    def open_spider(self, spider):
        self.file = open('items.jl', 'a', encoding='utf-8')

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):
        line = json.dumps(dict(item), ensure_ascii=False) + "\n" #Item을 한줄씩 구성
        self.file.write(line)
        return item

class MongoPipeline(object):

    collection_name = 'scrapy_items'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE', 'items')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        self.db[self.collection_name].insert_one(dict(item))
        return item