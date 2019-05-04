from mrjob.job import MRJob
import re
import operator
from mrjob.step import MRStep

class First_join(MRJob):
    Dict= {}
    def mapper1(self):
        with open("Initial_Filtering.csv") as file:
            for line in file:
                line1= line.replace('null', '')
                fields= line1.replace('\"', '').split(",")
                hash= fields[0]
                value= fields[1]
                publicKey= fields[2]
                self.Dict[hash]= value



    def mapper2(self, _, line):
        fields= line.split(",")
        try:
            if len(fields) == 3:
                txid = fields[0]
                tx_hash = fields[1]
                vout = fields[2]
                if txid in self.Dict:
                    yield(None,'{},{},{}'.format(txid,tx_hash,vout))
        except:
            pass

    def reduc(self):
        return [MRStep(mapper1= self.mapper1, mapper= self.mapper2)]

        if __name__ == '__main__':
        First_join.run()
