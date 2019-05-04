from mrjob.job import MRJob
import re
from mrjob.step import MRStep

class mapper1(MRJob):
    Dict= {}
    def mapper1(self):
        with open("First_Join.csv") as file:
            for line in file:
                try:
                    line1 = line.replace('null	', '')
                    fields = line1.replace('\"', '').split(",") # replace "
                    if len(fields)==3:
                        tx_hash = fields[1]
                        vout = fields[2]
                        self.Dict[tx_hash] = vout
                except:
                    pass
    def mapper2(self, _, line):
        fields = line.split(',')
        try:
            hash = fields[0]
            value = fields[1]
            n = fields[2]
            publicKey = fields[3]
            if (hash in self.Dict and n in self.Dict[hash]):
                yield(None,(publicKey,float(value)))
        except:
            pass

    def reducer(self, _, values):
        sorteds = sorted(values, reverse = True, key = lambda x :x[1])[:10]
        rank = 0
        for i in sorteds:
            rank += 1
            yield(rank,'{} - {}'.format(i[0],i[1]))
