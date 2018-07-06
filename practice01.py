from pyspark import SparkConf, SparkContext


def clean(x):
        return([xx.replace('"', '') for xx in x])

conf = SparkConf().setMaster("local[*]").setAppName("Practice 01")
sc = SparkContext(conf = conf)

data = sc.textFile("file:///projects/sparkCourse/practiceData/2012-10-01.csv")

content = data.map(lambda x: x.split(',')).map(clean)

# package_count = content.map(lambda x: (x[7], 1)).reduceByKey(lambda a,b: a+b)

# Counts up the keys, regardless of the value of those keys
# package_count = content.map(lambda x: (x[6], x[7])).sortByKey(0).take(100)

package_count = content.filter(lambda x: x[6] == 'mosaic' and x[7] == '0.6-2').take(10)

print (package_count)





