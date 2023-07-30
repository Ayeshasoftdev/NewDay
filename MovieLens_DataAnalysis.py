#!/usr/bin/env python
# coding: utf-8

# In[54]:


from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col
from pyspark.sql.functions import dense_rank

spark = SparkSession.builder\
        .appName("test")\
        .master("local")\
        .getOrCreate()


# In[34]:


# Reading .dat files to spark dataframes for availables datasets, we will use these dataframes for further analysis


# In[35]:


#change the path as required

unames = ['user_id', 'gender', 'age', 'occupation', 'zip']
users_sdf = spark.read.format("csv").option("delimiter", "::").option("inferSchema", "true").load(r"D:\Ay_Projects\newday\ml-1m\users.dat")
u_cols = users_sdf.columns
users_sdf = users_sdf.select([col(u_cols[i]).alias(unames[i]) for i in range(len(unames))])


# In[36]:


users_sdf.printSchema()


# In[37]:


#change the path as required

rnames = ['user_id', 'movie_id', 'rating', 'timestamp']
ratings_sdf = spark.read.format("csv").option("delimiter", "::").option("inferSchema", "true").load(r"D:\Ay_Projects\newday\ml-1m\ratings.dat")
ratings_sdf = ratings_sdf.select([col(u_cols[i]).alias(rnames[i]) for i in range(len(rnames))])


# In[38]:


ratings_sdf.printSchema()


# In[39]:


#change the path as required

mnames = ['movie_id', 'title', 'genres']
movies_sdf = spark.read.format("csv").option("delimiter", "::").option("inferSchema", "true").load(r"D:\Ay_Projects\newday\ml-1m\movies.dat")
movies_sdf = movies_sdf.select([col(u_cols[i]).alias(mnames[i]) for i in range(len(mnames))])


# In[40]:


movies_sdf.printSchema()
movies_sdf.createOrReplaceTempView("movies")
ratings_sdf.createOrReplaceTempView("ratings")
users_sdf.createOrReplaceTempView("users")


# # SPARK SQL: SOLUTION #1

# In[41]:


#Problems 2:   
#Creates a new dataframe, which contains the movies data and 3 new columns max, min and 
#average rating for that movie from the ratings data.


RatingAggregation_df = spark.sql("""select m.*, MAX(r.rating) as min_rating, MIN(r.rating) as max_rating, AVG(r.rating) as avg_rating from movies m
join ratings r on m.movie_id = r.movie_id
--where m.movie_id = '3793'
group by m.movie_id, m.title, m.genres
--order by m.movie_id
""")


# In[42]:


#Problems 3:   
#Create a new dataframe which contains each user’s (userId in the ratings data) top 3 movies 
#based on their rating.

TopRatings_df = spark.sql(""" select * from (select *,dense_rank() OVER (partition by user_id order by rating desc) rnk from ratings)
where rnk <=3
""")


# # Python PySpark Code: SOLUTION #2

# In[43]:


#Problems 2:   
#Creates a new dataframe, which contains the movies data and 3 new columns max, min and 
#average rating for that movie from the ratings data.

joined_df = movies_sdf.alias("movies").join(ratings_sdf.alias("ratings"), movies_sdf.movie_id == ratings_sdf.movie_id, "inner")\
            

# Apply the necessary aggregations
aggregated_df = joined_df.groupBy("movies.movie_id", "title", "genres") \
    .agg(
        F.max("ratings.rating").alias("max_rating"),
        F.min("ratings.rating").alias("min_rating"),
        F.avg("ratings.rating").alias("avg_rating")
    )


# In[49]:


#Problems 3:   
#Create a new dataframe which contains each user’s (userId in the ratings data) top 3 movies 
#based on their rating.

window = Window.partitionBy("user_id").orderBy(F.col("rating").desc())
ranked_df = ratings_sdf.withColumn("rnk", F.dense_rank().over(window))
top_3_ratings_df = ranked_df.where(F.col("rnk") <= 3)
top_3_ratings_df = top_3_ratings_df.select("user_id","movie_id","rating")


# In[45]:


# Write final dataframes for both problems to parquet format


# In[46]:


#original dataframes
movies_sdf.write.parquet("movies_dataset.parquet", mode="overwrite")
ratings_sdf.write.parquet("ratings_dataset.parquet", mode="overwrite")
users_sdf.write.parquet("users_dataset.parquet", mode="overwrite")


# In[50]:


# resulant dataframes
aggregated_df.write.parquet("result_aggregated.parquet", mode="overwrite")
top_3_ratings_df.write.parquet("top3_ratings.parquet", mode="overwrite")


# In[ ]:




