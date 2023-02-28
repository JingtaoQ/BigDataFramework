## connect to AWS
```
ssh -i /Users/douzi/Desktop/S9/BigDataFramework/aws/key-de2-jingtao.pem hadoop@ec2-44-204-221-105.compute-1.amazonaws.com

```


## input the csv
````
df = spark.read.csv("s3://s3-de2-jingtao/input/movielens/movies.csv", header = True, inferSchema = True)
````


- Movie ID--df
- Movie name--df
- Year of release--df
- Number of ratings--account of ratings
- Genre--df
- Rating average--ratings

## input libraries

````
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import regexp_replace
from pyspark.sql import functions as F
from pyspark.sql.functions import length, expr
from pyspark.sql.functions import col
from pyspark.sql import functions
from pyspark.sql.functions import split
from pyspark.sql.functions import collect_list
````

spark = SparkSession.builder.appName("Example").getOrCreate()
df = spark.read.csv("data.csv", header=True, inferSchema=True)
new_df = df.select("column_name1", "column_name2")

## modify the data format

```
ratings_data = ratings.withColumn("date", F.from_unixtime(ratings.timestamp)) 修正时间

best_movies = best_movies.withColumn("year",F.from_unixtime(best_movies.year))
```

## Average raing of each movie
```
avg=new_ratings.groupBy("movieId").avg("rating") 
sum=new_ratings.sum("movieId") 
````
### Sum people of rating for each movie
```
sum=new_ratings.groupBy(col("movieId")).count() //find the total number of people who have scored each movie

count = sum.join(avg1, "movieId")
count=count.withColumnRenamed("count", "NumberRatings")
```
![count](https://github.com/JingtaoQ/BigDataFramework/blob/main/count%20people.png)

## Remove () using regular
```
df1 = df.withColumn("year", regexp_replace(df["title"], "[^0-9]", ""))用于将字符串中匹配给定正则表达式的部分替换为指定的字符串
```

### Seperate the year and movie name 
```
df2 = df1.withColumn("Movie", substring(df["title"], 1, -6))
df2=df1.withColumn("Movie", expr("substring(title, 1, length(title)-6)"))//内容减去后6位 新建列表
df2=df2.drop("title")
````

## Reordeing
````
order=["movieId", "Movie", "year", "NumberRatings", "genres", "Rating average"]
 df3=df4[order]
df3.show(5)
```

## Merge 2 dataframe in the 1
```
df3 = df2.join(avg1,df2["movieId"] == avg1["movieId"],"inner")//Same data, no de-duplication of column names

df4 = df2.join(avg1, "movieId")  --count

df5=df4[order]
````

## Load the data into aws
```
df5.write.csv("s3://s3-de2-jingtao/output/movielens/")

df5.write.parquet("s3://s3-de2-jingtao/output/movielens/movie.parquet")//No spaces before storage
```

![dt](https://github.com/JingtaoQ/BigDataFramework/blob/main/dt.out.png)
# -----part 2----

## load the csv from aws
```
data = spark.read.csv("s3://s3-de2-jingtao/output/movielens/mivie/", header = True, inferSchema = True)
````
## Creare a view
```
dt = spark.read.parquet("s3://s3-de2-jingtao/output/movielens/movie.parquet/")
data.createOrReplaceTempView("df6") //建立视图
```

## Best movie per year

````
dt1.filter(dt1.NumberRatings > 515) \
   .groupBy("year", "Movie") \
   .agg(max("AvgRating").alias("rating")) \
   .sort(desc("rating")) \
   .select("year", "Movie", "rating") \
   .show(5)
````
![best movie](https://github.com/JingtaoQ/BigDataFramework/blob/main/Best%20movie%20per%20year.png)

## Best movie per genre
### There are 18 types, so do 18 selects to filter out the data that contains a field in the genre

````
dt1.where(dt1['genres'].contains('Western')) \
   .filter(dt1.NumberRatings > 515) \
   .groupBy("year", "Movie") \
   .agg(max("AvgRating").alias("rating")) \
   .sort(desc("rating")) \
   .select("year", "Movie", "rating") \
   .show(1,truncate = False)           
````
![best per genre](https://github.com/JingtaoQ/BigDataFramework/blob/main/Children.png)

## For 'action' movie per year 

````
dt1.where(dt1['genres'].contains('Action')) \
   .groupBy('year') \
   .agg(collect_list('Movie').alias('Movies'), collect_list('genres').alias('Genres')) \
   .orderBy('year') \
   .select('year', 'Movies', 'Genres') \
   .show(5)
````
![action](https://github.com/JingtaoQ/BigDataFramework/blob/main/Only%20action%20per%20year.png)

## Best romance per year 

```
dt1.where(dt1['genres'].contains('Romance')) \
   .filter(dt1.NumberRatings > 515) \
   .groupBy("year" , "Movie") \
   .agg(max("AvgRating").alias("rating")) \
   .sort(desc("rating")) \
   .select("year", "Movie", "rating") \
   .show(10)
   ```
![romance](https://github.com/JingtaoQ/BigDataFramework/blob/main/Best%20Romance.png)