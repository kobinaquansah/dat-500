from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):
    def mapper(self, _, line):
        entry = line.split(',')
        try:
            yield entry[0], entry[1:]
        except:
            pass

    def reducer(self, key, values):
        yield key, list(values)

if __name__ == '__main__':
    MRWordFrequencyCount.run()