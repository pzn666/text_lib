from pymongo import MongoClient
from pymongo import errors
from pymongo.collection import Collection
from token_base import TokenBase
from pprint import pprint
import pymongo
import multiprocessing as mp
import timer
import file

FOCAL_AUTH = {
    'account': 'pzn666',
    'password': 'xul4jp6cl4666'
}
READ_LIMIT = 20000
CHUNK_NUM = 10


class Tokenizer(TokenBase):
    def __init__(self, host='localhost', db='focal', collection='train_data'):
        super(Tokenizer, self).__init__()
        self.host = host
        self.db = db
        self.collection = collection
        self.exclude_list = []
        __stopwords = file.read_file(self.STOPWORD_PATH)
        for word in __stopwords:
            self.exclude_list.append(word.replace('\n', ''))
        self.exclude_list += self.EXCLUDE_LIST

    @timer.clocktimer
    def tokenize(self, context='', query={}):
        if not context:
            docs = self.__read_docs(limit=READ_LIMIT, query=query)
            pool = mp.Pool(mp.cpu_count())
            doc_chunks = list(self.__split_chunks(READ_LIMIT // CHUNK_NUM, docs))
            for i, chunk in enumerate(doc_chunks):
                processed_list = [x for x in pool.map(self.jieba_tokenize, chunk) if x]
                file_name = '/terms/terms_' + str(i)
                self.write_json(processed_list, file_name)
        else:
            return self.jieba_tokenize(context)

    @timer.clocktimer
    def tokenize_pos(self, context=''):
        if not context:
            docs = self.__read_docs(limit=1000)
            doc_chunks = list(self.__split_chunks(1000 // CHUNK_NUM, docs))
            pool = mp.Pool(mp.cpu_count())
            for i, chunk in enumerate(doc_chunks):
                pos_list = pool.map(self.jieba_tokenize_pos, chunk)
                file_name = '/pos_terms/pos_terms_' + str(i)
                self.write_json(pos_list, file_name)
        else:
            return self.jieba_tokenize_pos(context)

    def __read_docs(self, query, limit=1, last=False):
        client = MongoClient(self.host, 27017)
        col = client[self.db][self.collection]
        docs = []
        if not last:
            docs = col.find(query, {'context': 1, '_id': 0}).limit(limit)
        else:
            docs = col.find(query, {'context': 1, '_id': 0}).sort(
                [['_id', pymongo.DESCENDING]]).limit(limit)
        return list(docs)

    def __split_chunks(slef, n, input_list):
        for i in range(0, len(input_list), n):
            results = [data['context'] for data in input_list[i:i + n]]
            yield results


# query = {}
# tokenizer = Tokenizer()
# tokenizer.tokenize(query=query)
