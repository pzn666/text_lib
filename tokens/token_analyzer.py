import multiprocessing as mp
import os
import json
import file
import time


class TokenAnalyzer:
    def analysis(self):
        start_time = time.time()
        pool = mp.Pool(mp.cpu_count())
        term_list = self.__read_all_token()
        print(len(term_list))
        self.__build_term_stats(term_list)
        print("--- %s seconds ---" % (time.time() - start_time))

    def __build_term_stats(self, term_list):
        stats = {}
        for term in term_list:
            if term not in stats:
                stats[term] = 1
            else:
                stats[term] += 1
        total_term_num = len(term_list)
        for key in stats:
            stats[key] = stats[key] / total_term_num
        maxium_key = max(stats, key=stats.get)
        print(maxium_key, stats[maxium_key])
        # print(len(stats.keys()))
        # return stats

    def __read_all_token(self):
        folder_path = os.path.dirname(os.path.realpath(__file__)) + '/terms'
        term_list = []
        for f in os.listdir(folder_path):
            path = os.path.join(folder_path, f)
            if os.path.isfile(path) and '.json' in path:
                term_list += file.read_file(path)
        return term_list


# analyzer = TokenAnalyzer()
# analyzer.analysis()
