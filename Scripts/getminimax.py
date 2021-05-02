#import numpy as np
from copy import copy
from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):
    def mapper(self, _, line):
        try:
            line = line.replace('$','')
            line = line.replace(".$","")
            line = line.strip()
            splitline = eval(line.split('\t')[1])[0]
            splitline = list(map(lambda x: float(x),splitline))
            for i in range(len(splitline)):
                yield (int(i),(float(splitline[i])))
        except:
            pass

    def reducer(self, key, values):
        list1 = list(values)
        yield key, (min(list1), max(list1))

if __name__ == '__main__':
    MRWordFrequencyCount.run()

















