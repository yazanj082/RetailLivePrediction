
### this should run inside the spark-master container

from pyspark.sql import SparkSession

# Configure Spark to connect to the remote cluster
spark = SparkSession.builder \
    .appName("LiveDebuggingApp") \
    .master("spark://localhost:7077") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

# Test the connection
print("Spark UI URL:", spark.sparkContext.uiWebUrl)

# Example DataFrame operation
data = [("Alice", 29), ("Bob", 31), ("Cathy", 25)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

df.show()
