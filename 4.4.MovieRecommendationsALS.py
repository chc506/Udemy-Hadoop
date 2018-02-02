from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS  #MLLib,ALS recommadation algorithm
from pyspark.sql import Row
from pyspark.sql.functions import lit #put constant value in rows




# Load up movie ID -> movie name dictionary
def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1].decode('ascii', 'ignore')
    return movieNames



# Convert u.data lines into (userID, movieID, rating) rows
def parseInput(line):
    fields = line.value.split()
    return Row(userID = int(fields[0]), movieID = int(fields[1]), rating = float(fields[2]))




if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("MovieRecs").getOrCreate()

    
   
    # Load up our movie ID -> name dictionary
    movieNames = loadMovieNames()

   
    
    # Get the raw data
    #   don't use sparkcontext as before;
    #   actually return a dataframe. then use .rdd to convert it to rdd
    lines = spark.read.text("hdfs:///user/maria_dev/ml-100k/u.data").rdd




    # Convert it to a RDD of Row objects with (userID, movieID, rating)
    ratingsRDD = lines.map(parseInput)





    # Convert to a DataFrame and cache it
    #   this need to be used more than once
    ratings = spark.createDataFrame(ratingsRDD).cache()
    
    
    
    

    # Create an ALS collaborative filtering model from the complete data set
    als = ALS(maxIter=5, regParam=0.01, userCol="userID", itemCol="movieID", ratingCol="rating")
    model = als.fit(ratings)  #train







# fabricate a user 0 in u.data, who likes science fiction but not like historical drama
# recommend movies to this user 0( actually predict user 0's rating on each movie that he has never seen)
    
    
    
    
    # Print out ratings from user 0:
    print("\nRatings for user ID 0:")
    userRatings = ratings.filter("userID = 0")
    for rating in userRatings.collect():
        print movieNames[rating['movieID']], rating['rating']






    print("\nTop 20 recommendations:")

# only predict user 0's rating on movies with more than 100 ratings, so hvae a reasonabale amount of data
    
    
    # Find movies rated more than 100 times
    ratingCounts = ratings.groupBy("movieID").count().filter("count > 100")
    
    
    
    # Construct a "test" dataframe for user 0 with every movie rated more than 100 times
    #   two colums: each movieID(>100), userID=0
    #   go through eacn movie, predict userID=0 user' rating on them
    popularMovies = ratingCounts.select("movieID").withColumn('userID', lit(0))

  
    
    
    
    # Run our model on that list of popular movies for user ID 0
    #   predict ratings for these movieID and userID pairs
    recommendations = model.transform(popularMovies)
    
    
    

    # Get the top 20 movies with the highest predicted rating for this user
    topRecommendations = recommendations.sort(recommendations.prediction.desc()).take(20)





    for recommendation in topRecommendations:
        print (movieNames[recommendation['movieID']], recommendation['prediction'])

    
    
    spark.stop()
