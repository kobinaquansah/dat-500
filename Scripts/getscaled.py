import numpy as np
#from copy import copy
from mrjob.job import MRJob

with open("fu.txt","r") as file:
    data = file.read()
data = data.strip()
a = data.split('\n')
nest = [i.split('\t') for i in a]
nest = [nest[i][1] for i in range(len(nest))]
nest = [eval(nest[i]) for i in range(len(nest))]
data = np.array(nest)


class MRWordFrequencyCount(MRJob):
    def mapper(self, _, line):
        try:
            line = line.replace('$','')
            line = line.replace(".$","")
            line = line.strip()
            splitline = eval(line.split('\t')[1])[0]
            line1 = eval(line.split('\t')[0])
            splitline = list(map(lambda x: float(x),splitline))
            dist = np.array(splitline) - data[:,0].T
            rng = data[:,1].T - data[:,0].T
            a = list(dist/rng)
            #a = [i if not (np.isnan(i) or np.isinf(i)) else 0 for i in a]
            #yield line1, a
            yield line1, a
        except:
            pass

    def reducer(self, key, values):
        yield (key, list(values))

if __name__ == '__main__':
    MRWordFrequencyCount.run()