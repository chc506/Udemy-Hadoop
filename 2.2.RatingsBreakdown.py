

from mrjob.job import MRJob
from mrjob.step import MRStep



class RatingsBreakdown(MRJob): #inherit MRJob
    
   
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings)
        ]

    
    
    
   
    def mapper_get_ratings(self, _, line):  #self是面向对象，_是将多个MapReduce任务组成链时，从前一个reducer传入的key；line是每一行读入的数据
        (userID, movieID, rating, timestamp) = line.split('\t')  #tuple；
        yield rating, 1 #mapper输出键值对；值可以是tuple

   
    def reducer_count_ratings(self, key, values):#values是list，iterable value
        yield key, sum(values)



if __name__ == '__main__':  #如果从command-line执行该script，用于在individual节点上执行特定的mapper或reducer；一般都是执行于streaming context
    RatingsBreakdown.run()  #启动
