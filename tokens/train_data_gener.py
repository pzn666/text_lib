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
    'project_terms': 'project_terms',
    'data': 'news_data',
    'train_data': 'train_data',
    'train_data_vec': 'train_data_vec'
}


class TrainDataGener:
    def __init__(self, config):
        self.config = config

    def __read_project_terms(self):
        client = MongoClient(self.config['host'], self.config['port'])
        collection = client[self.config['db']][self.config['project_terms']]
        terms = collection.find({}, {'_id': 0})
        return list(terms)

    @timer.clocktimer
    def __extract_project_terms(self, datas):
        result = {}
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
            invalid = 0
            for keyword in doc['custom_corpus']['keyword']:
                if doc['context'].count(keyword) < 2:
                    invalid += 1
            if len(doc['custom_corpus']['keyword']['pos']) > invalid:
                result[doc['url']] = doc
        for data in datas:
            for key in data:
                if key == 'account' or key == 'project_name':
                    continue
                pos, neg = self.__parse_logic(data[key])
                query = {
                    '$text': {}
                }
                search_str = ''
                if len(pos):
                    search_str += ' '.join(pos)
                if len(neg):
                    search_str += ' -'.join(neg)
                query['$text']['$search'] = search_str
                docs = data_col.find(query)
                for doc in docs:
                    doc_id = doc['url']
                    if doc_id in result:
                        exist_doc = result[doc_id]
                        if data['account'] not in exist_doc['custom_corpus']['account']:
                            exist_doc['custom_corpus']['account'].append(data['account'])
                        if data['project_name'] not in exist_doc['custom_corpus']['project_name']:
                            exist_doc['custom_corpus']['project_name'].append(data['project_name'])
                        exist_doc['custom_corpus']['keyword']['pos'] = self.__list_concat(
                            exist_doc['custom_corpus']['keyword']['pos'], pos)
                        exist_doc['custom_corpus']['keyword']['neg'] = self.__list_concat(
                            exist_doc['custom_corpus']['keyword']['neg'], neg)
                    else:
                        doc['custom_corpus'] = {
                            'account': [data['account']],
                            'project_name': [data['project_name']],
                            'keyword': {
                                'pos': pos,
                                'neg': neg
                            }
                        }
                        result[doc['url']] = doc
        return result

    @timer.clocktimer
    def __insert_mongo(self, docs, col):
        client = MongoClient(self.config['host'], self.config['port'])
        bulk = col.initialize_unordered_bulk_op()
        try:
            for key in docs:
                bulk.find({'url': key}).upsert().replace_one(docs[key])
            bulk.execute()
            return True
        except Exception as e:
            return False

    def __parse_logic(self, logic):
        pos, neg = [], []
        replace_strs = ['(', ')', 'and', 'or']
        not_indexs = [s.start() + 3 for s in re.finditer('not', logic.lower())]
        for idx in not_indexs:
            end_idx = logic[idx:].find(')') + idx + 1
            not_str = logic[idx:end_idx][2:-1]
            not_str = not_str.lower().replace('and', '').replace('or', '')
            neg += not_str.split()
            logic = logic[:idx - 3] + ' ' + logic[end_idx:]
        for s in replace_strs:
            logic = logic.lower().replace(s, ' ')
        pos += logic.split()
        return pos, neg

    def __list_concat(self, a, b):
        return list(set().union(a, b))

    @timer.clocktimer
    def gen_train_data(self):
        raw_datas = self.__read_project_terms()
        docs = self.__extract_project_terms(raw_datas)
        result = self.__insert_mongo(
            docs, client[self.config['db']][self.config['train_data']])
        return result


gener = TrainDataGener(mongo_config)
gener.gen_train_data()
