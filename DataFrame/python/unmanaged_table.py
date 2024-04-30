from pyspark.sql import SparkSession

spark: SparkSession = (
    SparkSession.builder.config("spark.driver.host", "eddieK")
    .appName("UnManagedTable")
    .getOrCreate()
)

spark.sql(
    """
    CREATE TABLE managed_us_delay_flights_tbl (
        date        STRING,
        delay       INT,
        distance    INT,
        origin      STRING,
        destination STRING
    ) USING csv OPTIONS (PATH '../data/departuredelays.csv')
    """
)

file = "data/departuredelays.csv"
schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
flights_df = spark.read.csv(file, schema=schema)
flights_df.write.option("path", "/tmp/data/us_flights_delay").saveAsTable(
    "managed_us_delay_flights_tbl"
)
