from mrjob.job import MRJob
from difflib import SequenceMatcher

#inputs = input('Enter text: ')
#with open('whatever.txt',"w") as file:
#        file.write(inputs)

class MRWordFrequencyCount(MRJob):

    def max5(self,an):
        an.sort(reverse=True)
        return list(map(lambda x: x,an ))[0:5]
   # def inputs(self)
        #return input('Enter text: ')
        #with open('whatever.txt',"w") as file:
        #       file.write(inputs)

    #try:
       # with open('whatever.txt',"r") as file:
      #      vari = file.read().strip()
    #except:
     #   print('This is exception')

    def mapper(self, _, line):
        splitline = line.split(',')

        song_name = str(splitline[1]).lower()
        song_id = str(splitline[0])

        score = SequenceMatcher(a=song_name, b='rain').ratio()
        yield 0, (score,(song_id,song_name))

    def reducer(self, key, values):
        values = list(values)
        yield key, self.max5(values)

if __name__ == '__main__':
    MRWordFrequencyCount.run()