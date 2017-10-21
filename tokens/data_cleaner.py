from pymongo import MongoClient
from pprint import pprint
from token_base import TokenBase
from word2vec import WordEmbedder
from bson import Binary
import pickle
import os
import re
import timer

mongo_config = {
    'host': 'localhost',
    'port': 27017,
    'db': 'focal',
    'data': 'news_data',
    'train_data': 'key_train_data'
}


class DataCleaner:
    def __init__(self, config):
        self.config = config

    def __extract_keywords(self):
        client = MongoClient(self.config['host'], self.config['port'])
        data_col = client[self.config['db']][self.config['data']]
        train_col = client[self.config['db']][self.config['train_data']]
        key_docs = data_col.find({'keyword': {'$nin': [{}, None]}})
        for doc in key_docs:
            pos = []
            for key in doc['keyword']:
                if key == 'keyword':
                    for inner_key in doc['keyword']['keyword']:
                        pos += doc['keyword']['keyword'][inner_key]
                else:
                    pos += doc['keyword'][key]
                # for word in temp:
                #     pos += word.split()
                doc['custom_corpus'] = {
                            'account': [],
                            'project_name': [],
                            'keyword': {
                                'pos': list(set(pos)),
                                'neg': []
                            }
                        }
            yield doc

    def __filter_by_jieba(self, docs_gen):
        tokenizer = TokenBase()
        for doc in docs_gen:
            pos_set = set(doc['custom_corpus']['keyword']['pos'])
            terms_set = set(
                tokenizer.jieba_tokenize(doc['context'], ex_stop=True))
            if bool(terms_set.intersection(pos_set)):
                yield doc

    def __insert_mongo(self, docs_gen, bulk=False):
        """
        TODO: speed up
        """
        client = MongoClient(self.config['host'], self.config['port'])
        col = client[self.config['db']][self.config['train_data']]
        if bulk:
            bulk = col.initialize_unordered_bulk_op()
            for key in docs:
                bulk.find({'url': key}).upsert().replace_one(docs[key])
            bulk.execute()
        else:
            col.insert_many(list(docs_gen))

    @timer.clocktimer
    def gen_train_data(self):
        extracted_docs = self.__extract_keywords()
        jieba_docs = self.__filter_by_jieba(extracted_docs)
        result = self.__insert_mongo(jieba_docs)
        print('Data cleanning done')


gener = DataCleaner(mongo_config)
gener.gen_train_data()
