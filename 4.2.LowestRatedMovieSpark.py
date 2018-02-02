from pyspark import SparkConf, SparkContext





# This function just creates a Python "dictionary" we can later
# use to convert movie ID's to movie names while printing out
# the final results.

#on local disk,
def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames






# Take each line of u.data and convert it to (movieID, (rating, 1.0))
# This way we can then add up all the ratings for each movie, and
# the total number of ratings for each movie (which lets us compute the average)
def parseInput(line):
    fields = line.split()
    return (int(fields[1]), (float(fields[2]), 1.0))  #tuple contains tuple 




if __name__ == "__main__":
    # The main script - create our SparkContext
    conf = SparkConf().setAppName("WorstMovies")
    sc = SparkContext(conf = conf)



    # Load up our movie ID -> movie name lookup table
    movieNames = loadMovieNames()

   
    
    # Load up the raw u.data file
    # lines: a RDD object
    lines = sc.textFile("hdfs:///user/maria_dev/ml-100k/u.data")



    # Convert to (movieID, (rating_value, 1.0))
    # function as a parameter
    movieRatings = lines.map(parseInput)
    
    
    
    # Reduce to (movieID, (sumOfRatings, totalRatings))
    #for each key, ues function to simulate what should do for each pair of two values of this key
    #and each value is a tuple
    ratingTotalsAndCount = movieRatings.reduceByKey(lambda movie1, movie2: ( movie1[0] + movie2[0], movie1[1] + movie2[1] ) )



    # Map to (movieID, averageRating)
    # mapValues: only operate on the value of each key-value
    averageRatings = ratingTotalsAndCount.mapValues(lambda totalAndCount : totalAndCount[0] / totalAndCount[1])

   
    
    # Sort by average rating
    #by default: ascending order
    sortedMovies = averageRatings.sortBy(lambda x: x[1])



    # Take the top 10 results
    results = sortedMovies.take(10)



    #this's python
    # Print them out:
    for result in results:
        print(movieNames[result[0]], result[1])
