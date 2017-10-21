from pymongo import MongoClient
from pprint import pprint
from token_base import TokenBase
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

    def __filter_by_keyword(self):
        client = MongoClient(self.config['host'], self.config['port'])
        data_col = client[self.config['db']][self.config['data']]
        train_col = client[self.config['db']][self.config['train_data']]
        key_docs = data_col.find({'keyword': {'$nin': [{}, None]}})
        for doc in key_docs:
            temp, pos = [], []
            for key in doc['keyword']:
                if key == 'keyword':
                    for inner_key in doc['keyword']['keyword']:
                        temp += doc['keyword']['keyword'][inner_key]
                else:
                    temp += doc['keyword'][key]
                for word in temp:
                    pos += word.split()
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

    def __clean_pos_by_word2vec(slef, docs_gen):
        from word2vec import WordEmbedder
        modal_path = '/Users/pzn666/Documents/WorkSpace/text_lib/word2vec/word2vec_models/vec_model.bin'
        embedder = WordEmbedder(modal_path)
        for doc in docs_gen:
            clean_pos = []
            pos = doc['custom_corpus']['keyword']['pos']
            for word in pos:
                if embedder.check_in_vocab(word):
                    clean_pos.append(word)
            if len(clean_pos):
                doc['custom_corpus']['keyword']['pos'] = clean_pos
                yield doc

    def __insert_mongo(self, docs_gen, bulk=False):
        """
        TODO: need speed up
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
    def gen_train_data(self, word2vec=True):
        extracted_docs = self.__filter_by_keyword()
        jieba_docs = self.__filter_by_jieba(extracted_docs)
        if word2vec:
            clean_docs = self.__clean_pos_by_word2vec(jieba_docs)
        else:
            pass
        result = self.__insert_mongo(clean_docs)
        print('Data cleanning done')


gener = DataCleaner(mongo_config)
gener.gen_train_data()
