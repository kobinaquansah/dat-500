import numpy as np
from copy import copy
from mrjob.job import MRJob
from mrjob.job import MRStep
entries = []
with open(r"half.txt",'r') as file:
    data = file.read()
    data = data.split('\n')
    data = [data[i].split('\t') for i in range(10)]
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
                   reducer=self.reducer)]

    def mapper(self, _, line):
        line1 = line.split('\t')[0]
        splitline = eval(line.split('\t')[1])[0]
        split_data= list(map(lambda x: float(x),splitline))
        y = split_data[0]
        split_data = split_data[1:]
        split_data = list(map(lambda x: float(x), split_data))
        for j in range(len(entries)):
            dist = np.mean((entries[j][1:] - np.array(split_data))**2)
            yield (keys1[j], (dist,line1))

    def reducer(self, key, values):
        yield (key, getminimum(list(values)))

if __name__ == '__main__':
    MRWordFrequencyCount().run()




