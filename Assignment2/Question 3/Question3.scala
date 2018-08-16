// Databricks notebook source
//Q3_1
//Store csvs in data frame 
val movies_df = sqlContext.read.format("csv").option("header", "true").load("/FileStore/tables/movies.csv")
val ratings_df = sqlContext.read.format("csv").option("header", "true").load("/FileStore/tables/ratings.csv")

//join two dataframes
val joined_df = movies_df.join(ratings_df, movies_df.col("movieId") === ratings_df.col("movieId"),"inner").select(movies_df("title"), ratings_df("movieId"),ratings_df("rating"))

//store the joined df in a temp table
joined_df.createOrReplaceTempView("joinedtable")

//q3_1_a query
val result1 = spark.sql("SELECT title,movieId,AVG(rating) as avg_rating from joinedtable GROUP BY movieId,title ORDER BY length(movieId),movieId")

//q3_1_b query 
val result2 = spark.sql("SELECT title,movieId,AVG(rating) as avg_rating from joinedtable GROUP BY movieId,title ORDER BY avg_rating limit 10")

//store the file on dbfs 
result1.coalesce(1).write.format("csv").option("header", "true").save("/FileStore/output/q3_1aoutput")
result2.coalesce(1).write.format("csv").option("header", "true").save("/FileStore/output/q3_1boutput")


// COMMAND ----------

//Q3_2
//Store csvs in data frame 
val movies_df = sqlContext.read.format("csv").option("header", "true").load("/FileStore/tables/movies.csv")
val ratings_df = sqlContext.read.format("csv").option("header", "true").load("/FileStore/tables/ratings.csv")
val tags_df = sqlContext.read.format("csv").option("header", "true").load("/FileStore/tables/tags.csv")

//join two dataframes
val join_df = movies_df.join(ratings_df, movies_df.col("movieId") === ratings_df.col("movieId"),"inner").join(tags_df, tags_df.col("movieId") === movies_df.col("movieId"),"inner").select(movies_df("title"), ratings_df("movieId"),ratings_df("rating"),tags_df("tag"),movies_df("genres"))

//store the joined df in a temp table
join_df.createOrReplaceTempView("joinedtable")

//q3_2 query
val result1 = spark.sql("""SELECT tag,title,movieId,AVG(rating) as avg_rating from joinedtable 
WHERE tag='action' GROUP BY movieId,title,tag ORDER BY length(movieId),movieId""")

//q3_3 query
val result2 = spark.sql("""SELECT tag,genres,title,movieId,AVG(rating) as avg_rating from joinedtable 
WHERE tag='action' and genres LIKE '%Thriller%' GROUP BY movieId,title,tag,genres
ORDER BY length(movieId),movieId""")


//store the file on dbfs 
result1.coalesce(1).write.format("csv").option("header", "true").save("/FileStore/output/q3_2aoutput")
result2.coalesce(1).write.format("csv").option("header", "true").save("/FileStore/output/q3_3boutput")


// COMMAND ----------


