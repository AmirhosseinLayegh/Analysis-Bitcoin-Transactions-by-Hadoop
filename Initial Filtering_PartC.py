from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import operator

Class Initial_Filtering(MRJob):

    def mapper(self, _, line):
        fields= line.split(",")
        try:
            if len(fields)== 4 :
                hash= fields[0]
                value= fields[1]
                n= fields[2]
                publicKey= fields[3]
                if (publicKey== "{1HB5XMLmzFVj8ALj6mfBsbifRoD4miY36v}"):
                    yield('null',(hash, value, n, publicKey)) #yielding null because we don't pass anything
        except:
            pass


    def reducer(self, key, value):
        for i in value:
            yield(None,'{},{},{}'.format(i[0], i[1], i[2], i[3]))

if __name__ == '__main__':
    Initial_Filtering.run()
