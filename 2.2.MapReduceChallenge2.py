from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings),
                   MRStep(reducer=self.reducer_sorted_output) #it's a list;each MRStep(,) represents a time MR job
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_count_ratings(self, key, values):
        yield str(sum(values)).zfill(5),key  #!!:streaming yeilds string, by default sorting  lexicographically
        
    def reducer_sorted_output(self, key, values):
        for v in values:
            yield v,key
        
        

if __name__ == '__main__':
    RatingsBreakdown.run()
