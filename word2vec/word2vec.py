from gensim.models import word2vec
from pprint import pprint
import multiprocessing as mp
import tensorflow as tf
import os
import json
import file
import timer
import time


class WordEmbedder:
    def __init__(self, model_path):
        self.model = word2vec.Word2Vec.load(model_path)

    def change_model(self, model_path):
        self.model = word2vec.Word2Vec.load(model_path)

    @timer.clocktimer
    def train(self, data_folder_path, model_path, online_train=False):
        sentences = self.__read_terms_data(data_folder_path)
        if online_train:
            self.model.build_vocab(sentences, update=True)
            self.model.train(
                sentences, total_examples=model.corpus_count,
                epochs=model.iter)
            self.model.save(model_path)
        else:
            model = word2vec.Word2Vec(sentences, size=250)
            model.save(model_path)

    def get_similarity(self, target_word):
        return self.model.most_similar(target_word)

    def get_similarities(self, target_words, threshold=0):
        result = {}
        for word in target_words:
            sims = self.model.most_similar(word)
            if threshold == 0:
                result[word] = sims
            else:
                valids = []
                for sim in sims:
                    if sim[1] >= threshold:
                        valids.append(sim)
                result[word] = valids
        return result

    def get_vectors(self, words):
        for word in words:
            if word in self.model.wv.vocab:
                yield(self.model[word])

    def check_in_vocab(self, word):
        if word in self.model.wv.vocab:
            return True
        else:
            return False

    def get_vocab_size(self):
        return len(self.model.wv.vocab)

    def get_similar_by_vec(self, vecs, n):
        return self.model.most_similar(positive=vecs, topn=1)

    def __read_terms_data(self, folder_path):
        datas = []
        for f in os.listdir(folder_path):
            path = os.path.join(folder_path, f)
            if os.path.isfile(path) and '.json' in path:
                datas += file.read_file(path)
        return datas

# check_words = ['蔡依林']
# model_path = os.path.dirname(os.path.realpath(__file__)) + '/word2vec_models/vec_model.bin'
# embedder = WordEmbedder(model_path)
# pprint(embedder.get_similarities(check_words, threshold=0.4).items())
# print(list(embedder.get_vectors(check_words)))
# folder_path = os.path.dirname(os.path.realpath(__file__)) + '/terms'
# embedder.train(folder_path, model_path)
# print(embedder.get_vocab_size(model_path))
# embedder.check_similar(check_words, model_path)
# print(embedder.get_similar_by_vec(embedder.get_vectors(check_words, model_path), 10, model_path))
