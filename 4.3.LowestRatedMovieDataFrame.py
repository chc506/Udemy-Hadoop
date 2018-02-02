#spark2.0:dataframe
from pyspark.sql import SparkSession #encompass spark context and sql context
from pyspark.sql import Row #dataframe of row objects
from pyspark.sql import functions #sql function




def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames




def parseInput(line):
    fields = line.split()
    #spark2.0: return row-object
    return Row(movieID = int(fields[1]), rating = float(fields[2]))




if __name__ == "__main__":
   
    # Create a SparkSession; didn't succed last time:create from a saved snapshot
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

    
    
    
    # Load up our movie ID -> name dictionary
    movieNames = loadMovieNames()




    # Get the raw data
    #   same to the spark 1.0, also use sparkContext's textfile interface
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.data")
    
    
    
    
    # Convert it to a RDD of Row objects with (movieID, rating)
    #   also function as parameter
    #   movies is also a RDD
    movies = lines.map(parseInput)
    
    
   
    
    
    # Convert RDD to a DataFrame
    movieDataset = spark.createDataFrame(movies)
    
    
    
    
    
    

    # Compute average rating for each movieID
    averageRatings = movieDataset.groupBy("movieID").avg("rating")





    # Compute count of ratings for each movieID
    counts = movieDataset.groupBy("movieID").count()





    # Join the two together (We now have movieID, avg(rating), and count columns)
    averagesAndCounts = counts.join(averageRatings, "movieID")






    # Pull the top 10 results
    topTen = averagesAndCounts.orderBy("avg(rating)").take(10)




    # this's python
    #   Print them out, converting movie ID's to names as we go.
    for movie in topTen:
        print (movieNames[movie[0]], movie[1], movie[2])

   
    
    # Stop the session
    spark.stop()
