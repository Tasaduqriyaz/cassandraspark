from pyspark.sql import SparkSession, Row
from pyspark.sql import functions
# We'll address the cassandra.cluster import problem

def parseInput(line):
    fields = line.split('|')
    return Row(user_id=int(fields[0]),
               age=int(fields[1]),
               gender=fields[2],
               occupation=fields[3]),
               zip=fields[4])

# Let's modify this to use Spark's connector instead of the Python driver
def create_keyspace_and_table(spark, keyspace):
    # Use Spark to create keyspace and table through the connector
    spark.sql("""
    CREATE KEYSPACE IF NOT EXISTS {0}
    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """.format(keyspace))
    
    # Create the users table
    spark.sql("""
    CREATE TABLE IF NOT EXISTS {0}.users (
        user_id INT PRIMARY KEY,
        age INT,
        gender TEXT,
        occupation TEXT,
        zip TEXT
    )
    """.format(keyspace))

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.\
        appName("CassandraExample").\
        config("spark.cassandra.connection.host", "127.0.0.1").\
        getOrCreate()
    
    # Create keyspace and table using Spark SQL
    create_keyspace_and_table(spark, "moviesdata")
    
    # Create an RDD from the raw text file stored in HDFS
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/cassandra/movies.user")
    # Convert each line to a Row object with the appropriate schema
    users = lines.map(parseInput)
    # Convert the RDD of Rows to a DataFrame
    usersDataset = spark.createDataFrame(users)
    
    # Write the DataFrame into Cassandra
    usersDataset.write.\
        format("org.apache.spark.sql.cassandra").\
        mode('append').\
        options(table="users", keyspace="moviesdata").\
        save()
