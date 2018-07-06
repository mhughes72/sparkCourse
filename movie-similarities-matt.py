import sys
from pyspark import SparkConf, SparkContext
from math import sqrt

def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.ITEM", encoding='ascii', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

def filterDuplicates( userRatings ):
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return movie1 < movie2

def makePairs( userRatings ):
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return ((movie1, movie2), (rating1, rating2))
    
    
def computeCosineSimilarity(ratingPairs):
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in ratingPairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))

    return (score, numPairs)

    
conf = SparkConf().setMaster("local[*]").setAppName("MovieSimilarities")
sc = SparkContext(conf = conf)


print("\nLoading movie names...")
nameDict = loadMovieNames()


data = sc.textFile("file:///projects/sparkCourse/ml-100k/u.data")
print ("DATA: ")
print (data.take(10))


ratings = data.map(lambda l: l.split()).map(lambda l: (int(l[0]), (int(l[1]), float(l[2]))))
print ("RATINGS: ")
print (ratings.take(10))

joinedRatings = ratings.join(ratings)
print ("JOINED: ")
print (joinedRatings.take(10))

uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

moviePairs = uniqueJoinedRatings.map(makePairs)

print ("MOVIE PAIRS: ")
print (moviePairs.take(10))

moviePairRatings = moviePairs.groupByKey()

print ("MOVIE PAIRS: RATINGS: ")
print (moviePairRatings.take(10))
moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()

print ("MOVIE PAIR SIMILARITIES: ")
print (moviePairSimilarities.take(10))

# Save the results if desired
#moviePairSimilarities.sortByKey()
#moviePairSimilarities.saveAsTextFile("file:///projects/sparkCourse/movie-sims.txt")

