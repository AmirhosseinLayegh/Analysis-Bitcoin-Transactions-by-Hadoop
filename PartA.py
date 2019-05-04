
from mrjob.job import MRJob
import re
import time

class partA(MRJob):
    def mapper(self, _, line):
        fields = line.split(",")
        try:
            time_epoch= int(fields[2])
            ynm= time.strftime("%Y-%m",time.gmtime(time_epoch))
            yield(ynm,1)
        except:
            pass
        

    def reducer(self,ynm,counts):
        yield (ynm, sum(counts))

if __name__ == '__main__':
    partA.JOBCONF= { 'mapreduce.job.reduces': '3' }
    partA.run()
