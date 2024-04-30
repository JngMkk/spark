import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark: SparkSession = (
    SparkSession.builder.config("spark.driver.host", "eddieK").appName("SparkSQLEx").getOrCreate()
)
file = "data/departuredelays.csv"

# * 스키마 추론. 더 큰 파일의 경우 스키마를 지정해주도록!
df = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load(file)

# * 임시 뷰 생성
df.createOrReplaceTempView("us_delay_flights_tbl")

# * 비행거리가 1000마일 이상인 모든 항공편
spark.sql(
    """
    SELECT
        distance,
        origin,
        destination
    FROM
        us_delay_flights_tbl
    WHERE
        distance >= 1000
    ORDER BY
        distance DESC
    """
).show(10)

df.select("distance", "origin", "destination").where("distance >= 1000").orderBy(
    F.desc("distance")
).show(10)

"""
+--------+------+-----------+
|distance|origin|destination|
+--------+------+-----------+
|    4330|   HNL|        JFK|
|    4330|   JFK|        HNL|
|    4330|   HNL|        JFK|
|    4330|   JFK|        HNL|
|    4330|   HNL|        JFK|
|    4330|   JFK|        HNL|
|    4330|   HNL|        JFK|
|    4330|   JFK|        HNL|
|    4330|   HNL|        JFK|
|    4330|   JFK|        HNL|
+--------+------+-----------+
only showing top 10 rows
"""

# * 샌프란시스코(SFO)와 시카고(ORD) 간 2시간 이상 지연이 있었던 모든 항공편
spark.sql(
    """
    SELECT
        date,
        delay,
        origin,
        destination
    FROM
        us_delay_flights_tbl
    WHERE
        delay > 120
        AND
        origin = 'SFO'
        AND
        destination = 'ORD'
    ORDER BY
        delay DESC    
    """
).show(10, False)

df.select("date", "delay", "origin", "destination").filter(
    (F.col("delay") > 120) & (F.col("origin") == "SFO") & (F.col("destination") == "ORD")
).sort(df.delay.desc()).show(10)

"""
+-------+-----+------+-----------+
|   date|delay|origin|destination|
+-------+-----+------+-----------+
|2190925| 1638|   SFO|        ORD|
|1031755|  396|   SFO|        ORD|
|1022330|  326|   SFO|        ORD|
|1051205|  320|   SFO|        ORD|
|1190925|  297|   SFO|        ORD|
|2171115|  296|   SFO|        ORD|
|1071040|  279|   SFO|        ORD|
|1051550|  274|   SFO|        ORD|
|3120730|  266|   SFO|        ORD|
|1261104|  258|   SFO|        ORD|
+-------+-----+------+-----------+
only showing top 10 rows
"""

# * 모든 미국 항공편에 매우 긴 지연(>= 6시간), 긴 지연(2 ~ 6시간) 등의 지연에 대한 표시를 레이블로 지정.
spark.sql(
    """
    SELECT
        delay,
        origin,
        destination,
        CASE WHEN delay >= 360 THEN 'Very Long Delays'
            WHEN delay >= 120 AND delay < 360 THEN 'Long Delays'
            WHEN delay >= 60 AND delay < 120 THEN 'Short Delays'
            WHEN delay > 0 AND delay < 60 THEN 'Tolerable Delays'
            WHEN delay = 0 THEN 'No Delays'
            ELSE 'Early'
        END AS flight_delays
    FROM
        us_delay_flights_tbl
    ORDER BY
        origin,
        delay DESC
    """
).show(10)

df.select(
    "delay",
    "origin",
    "destination",
    F.when(df.delay >= 360, "Very Long Delays")
    .when((df.delay >= 120) & (df.delay < 360), "Long Delays")
    .when((df.delay >= 60) & (df.delay < 120), "Short Delays")
    .when((df.delay > 0) & (df.delay < 60), "Tolerable Delays")
    .when(df.delay == 0, "No Delays")
    .otherwise("Early")
    .alias("flight_delays"),
).sort("origin", F.desc("delay")).show(10)

"""
+-----+------+-----------+-------------+
|delay|origin|destination|flight_delays|
+-----+------+-----------+-------------+
|  333|   ABE|        ATL|  Long Delays|
|  305|   ABE|        ATL|  Long Delays|
|  275|   ABE|        ATL|  Long Delays|
|  257|   ABE|        ATL|  Long Delays|
|  247|   ABE|        ATL|  Long Delays|
|  247|   ABE|        DTW|  Long Delays|
|  219|   ABE|        ORD|  Long Delays|
|  211|   ABE|        ATL|  Long Delays|
|  197|   ABE|        DTW|  Long Delays|
|  192|   ABE|        ORD|  Long Delays|
+-----+------+-----------+-------------+
only showing top 10 rows
"""
