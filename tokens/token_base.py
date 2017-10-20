import os
import json
import file
import jieba
import jieba.posseg
import timer


class TokenBase:
    def __init__(self):
        self.STOPWORD_PATH = os.path.dirname(os.path.realpath(__file__)) + '/stopword.txt'
        self.EXCLUDE_LIST = [
            '（', '〔', '［', '｛', '《', '【', '〖', '〈', '(', '[' '{', '<',
            '）', '〕', '］', '｝', '》', '】', '〗', '〉', ')', ']', '}', '>',
            '“', '‘', '『', '』', '。', '？', '?', '！', '!', '，', ',', '', '；',
            ';', '、', '：', ':', '……', '…', '——', '—', '－－', '－', '-', ' ',
            '「', '」', '／', '/', '．', "'", '\n']
        self.EXCLUDE_LIST_STOP = []
        __stopwords = file.read_file(self.STOPWORD_PATH)
        for word in __stopwords:
            self.EXCLUDE_LIST_STOP.append(word.replace('\n', ''))
        self.EXCLUDE_LIST_STOP += self.EXCLUDE_LIST

    def write_json(self, content, file_path):
        file_name = file_path + '.json'
        save_path = os.path.dirname(os.path.realpath(__file__)) + file_name
        file.write_file(save_path, content, as_json=True)

    def read_terms_data(self, folder_path):
        datas = []
        for f in os.listdir(folder_path):
            path = os.path.join(folder_path, f)
            if os.path.isfile(path) and '.json' in path:
                datas += file.read_file(path)
        return datas

    def migrate_raw_data(slef, data_list):
        result = []
        for data in data_list:
            result += data
        return result

    def jieba_tokenize(self, context, ex_stop=False):
        seg_set = set(list(jieba.cut(context)))
        if ex_stop:
            exclude_set = set(self.EXCLUDE_LIST_STOP)
        else:
            exclude_set = set(self.EXCLUDE_LIST)
        excluded_seg = list(seg_set - exclude_set)
        if excluded_seg:
            return excluded_seg

    def jieba_tokenize_pos(self, context):
        result = []
        pos_list = list(jieba.posseg.cut(context))
        for word, pos in pos_list:
            if pos[0] == 'n':
                result.append(word)
        return result
