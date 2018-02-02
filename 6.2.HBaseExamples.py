from starbase import Connection  #rest client for hbase with a python wrapper


# connect to the rest server on hbase
c = Connection("127.0.0.1", "8000")


# create a table(schema) named 'ratings'
# if table already exists, drop it
ratings = c.table('ratings')

if (ratings.exists()):
    print("Dropping existing 'ratings' table\n")
    ratings.drop()



# create a 'rating' column family
ratings.create('rating')





print("Parsing the ml-100k ratings data...\n")
ratingFile = open("D:\u.data", "r") # no chinese character




# import into hbase
# starbase batch interface
batch = ratings.batch()


# update batch with new rows
for line in ratingFile:
    (userID, movieID, rating, timestamp) = line.split()
    batch.update(userID, {'rating': {movieID: rating}})




ratingFile.close()



# write data to hbase through rest service
print ("Committing ratings data to HBase via REST service\n")
batch.commit(finalize=True)





# test query 
print ("Get back ratings for some users...\n")
print ("Ratings for user ID 1:\n")
print (ratings.fetch("1"))
print ("Ratings for user ID 33:\n")
print (ratings.fetch("33"))

ratings.drop()
