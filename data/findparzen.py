from copy import copy
from mrjob.job import MRJob
from mrjob.job import MRStep
import numpy as np
entries = []
with open(r"half.txt",'r') as file:
    data = file.read()
    data = data.split('\n')
    data = [data[i].split('\t') for i in range(100)]
    for i in range(len(data) -1):
        data1 = data[i][1].replace("[",'').replace("]",'').replace('"',"").split(',')
        data1 = list(map(lambda x: float(x), data1))
        entries.append(data1)
    keys1 = list(map(lambda x: x[0], data))

h = 0.000001

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
            dist = np.sum(np.exp(-((entries[j][1:]/np.array(h) - np.array(split_data)/np.array(h))**2)))
            yield (keys1[j], (dist,y))

    def reducer(self, key, values):
        values = list(values)
        values = np.array(values)
        y_vals = set(list(values[:,1]))
        for val in y_vals:
            output = values[np.array(list(map(lambda x: x[1] == val, values)))]
            output = sum(list(map(lambda x: x[0], output)))
            yield (key, (output, val))

    def reducer2(self, key, values):
        values = list(values)
        yield key, max(values)[1]

if __name__ == '__main__':
    MRWordFrequencyCount().run()