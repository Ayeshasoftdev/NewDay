# Movie Ratings Analysis with Spark
This repository contains a Spark job that performs an analysis on movie ratings data. The assignment involves four main tasks:

## Task 1: Read Data
The job reads the movies.dat, users.dat and ratings.dat files to create Spark DataFrames for further processing.

## Task 2: Movie Ratings Aggregation
The job creates a new DataFrame that contains the movies data and adds three new columns: max_rating, min_rating, and avg_rating for each movie. These columns represent the maximum, minimum, and average ratings given to that movie based on the data from the ratings.dat and movie.dat files.

## Task 3: User's Top 3 Movies
The job creates another DataFrame that contains each user's (identified by userId in the ratings.dat file) top 3 movies based on their highest ratings.

## Task 4: Write Data
The job writes out the original and new DataFrames in parquet format.

## How to Build and Run the Code

###  1. Spark-submit
To execute the assignment using spark-submit, follow the steps below:

- Make sure you have Apache Spark installed on your local machine. If not, download Spark first, and set the SPARK_HOME environment variable to the Spark installation directory.
- Submit the Spark job using the spark-submit command:
`bin\spark-submit --master local[*] --conf spark.pyspark.python=<path_to_python_executable> movie_ratings_analysis.py`

The Spark job will execute and perform the data analysis tasks.
- The resulting DataFrames will be saved in an parquet format.

### 2. Jupyter Notebook
You can open the file in Jupyter Notebook and execute the code directly within the notebook environment. This allows you to run the Spark job seamlessly using Jupyter Notebook.

## Important Notes
- This Spark job assumes that the SPARK_HOME environment variable is correctly set, pointing to your Spark installation directory.
- Make sure the Python version specified in the spark.pyspark.python configuration matches the Python version you intend to use for the analysis and there are no conflicts in python installation.


