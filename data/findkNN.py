from copy import copy
from mrjob.job import MRJob
from mrjob.job import MRStep
import numpy as np
entries = []
with open(r"half.txt",'r') as file:
    data = file.read()
    data = data.split('\n')
    data = [data[i].split('\t') for i in range(20)]
    for i in range(len(data) -1):
        data1 = data[i][1].replace("[",'').replace("]",'').replace('"',"").split(',')
        data1 = list(map(lambda x: float(x), data1))
        entries.append(data1)
    keys1 = list(map(lambda x: x[0], data))

def getminimum(entry):
    entry.sort()
    return list(map(lambda x: x[1],entry[1:6]))

class MRWordFrequencyCount(MRJob):
    vals = np.tile(10**6,5)
    vals = list(vals)

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer2)]

    def mapper(self, _, line):
        line1 = line.split('\t')[0]
        splitline = eval(line.split('\t')[1])[0]
        split_data= list(map(lambda x: float(x),splitline))
        y = split_data[0]
        split_data = split_data[1:]
        split_data = list(map(lambda x: float(x), split_data))
        for j in range(len(entries)):
            dist = np.mean((entries[j][1:] - np.array(split_data))**2)
            yield (keys1[j], (dist,y))

    def reducer(self, key, values):
        yield (key, getminimum(list(values)))

    def get_counts(self,entry):
        valset = set(entry[0])
        return max([(sum(np.array(entry[0]) == val),val) for val in valset])[1]

    def reducer2(self, key, values):
        values = list(values)
        yield key, self.get_counts(values)

if __name__ == '__main__':
    MRWordFrequencyCount().run()
