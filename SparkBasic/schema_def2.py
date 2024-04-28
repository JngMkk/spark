from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, desc, expr

# * 스키마 정의 후 Json 파일 읽기
schema = "Id INT, First STRING, Last STRING, Url STRING, Published STRING, Hits INT, Campaigns ARRAY<STRING>"


if __name__ == "__main__":
    spark: SparkSession = SparkSession.builder.appName("SchemaEx2").getOrCreate()
    blogs_df = spark.read.schema(schema=schema).json("data/blogs.json")
    blogs_df.show(truncate=False)
    blogs_df.printSchema()

    """
    +---+---------+-------+-----------------+---------+-----+----------------------------+
    |Id |First    |Last   |Url              |Published|Hits |Campaigns                   |
    +---+---------+-------+-----------------+---------+-----+----------------------------+
    |1  |Jules    |Damji  |https://tinyurl.1|1/4/2016 |4535 |[twitter, LinkedIn]         |
    |2  |Brooke   |Wenig  |https://tinyurl.2|5/5/2018 |8908 |[twitter, LinkedIn]         |
    |3  |Denny    |Lee    |https://tinyurl.3|6/7/2019 |7659 |[web, twitter, FB, LinkedIn]|
    |4  |Tathagata|Das    |https://tinyurl.4|5/12/2018|10568|[twitter, FB]               |
    |5  |Matei    |Zaharia|https://tinyurl.5|5/14/2014|40578|[web, twitter, FB, LinkedIn]|
    |6  |Reynold  |Xin    |https://tinyurl.6|3/2/2015 |25568|[twitter, LinkedIn]         |
    +---+---------+-------+-----------------+---------+-----+----------------------------+

    root
    |-- Id: integer (nullable = true)
    |-- First: string (nullable = true)
    |-- Last: string (nullable = true)
    |-- Url: string (nullable = true)
    |-- Published: string (nullable = true)
    |-- Hits: integer (nullable = true)
    |-- Campaigns: array (nullable = true)
    |    |-- element: string (containsNull = true)
    """

    print(blogs_df.columns)
    "['Id', 'First', 'Last', 'Url', 'Published', 'Hits', 'Campaigns']"

    blogs_df.select(expr("Hits * 2")).show(2)
    blogs_df.select(col("Hits") * 2).show(2)
    """
    +----------+
    |(Hits * 2)|
    +----------+
    |      9070|
    |     17816|
    +----------+
    only showing top 2 rows

    +----------+
    |(Hits * 2)|
    +----------+
    |      9070|
    |     17816|
    +----------+
    only showing top 2 rows
    """

    # * 블로그 우수 방문자를 계산하기 위한 식 표현
    blogs_df.withColumn("Big Hitters", expr("Hits > 10000")).show()
    blogs_df.withColumn("Big Hitters", col("Hits") > 10000).show()
    """
    +---+---------+-------+-----------------+---------+-----+--------------------+-----------+
    | Id|    First|   Last|              Url|Published| Hits|           Campaigns|Big Hitters|
    +---+---------+-------+-----------------+---------+-----+--------------------+-----------+
    |  1|    Jules|  Damji|https://tinyurl.1| 1/4/2016| 4535| [twitter, LinkedIn]|      false|
    |  2|   Brooke|  Wenig|https://tinyurl.2| 5/5/2018| 8908| [twitter, LinkedIn]|      false|
    |  3|    Denny|    Lee|https://tinyurl.3| 6/7/2019| 7659|[web, twitter, FB...|      false|
    |  4|Tathagata|    Das|https://tinyurl.4|5/12/2018|10568|       [twitter, FB]|       true|
    |  5|    Matei|Zaharia|https://tinyurl.5|5/14/2014|40578|[web, twitter, FB...|       true|
    |  6|  Reynold|    Xin|https://tinyurl.6| 3/2/2015|25568| [twitter, LinkedIn]|       true|
    +---+---------+-------+-----------------+---------+-----+--------------------+-----------+

    +---+---------+-------+-----------------+---------+-----+--------------------+-----------+
    | Id|    First|   Last|              Url|Published| Hits|           Campaigns|Big Hitters|
    +---+---------+-------+-----------------+---------+-----+--------------------+-----------+
    |  1|    Jules|  Damji|https://tinyurl.1| 1/4/2016| 4535| [twitter, LinkedIn]|      false|
    |  2|   Brooke|  Wenig|https://tinyurl.2| 5/5/2018| 8908| [twitter, LinkedIn]|      false|
    |  3|    Denny|    Lee|https://tinyurl.3| 6/7/2019| 7659|[web, twitter, FB...|      false|
    |  4|Tathagata|    Das|https://tinyurl.4|5/12/2018|10568|       [twitter, FB]|       true|
    |  5|    Matei|Zaharia|https://tinyurl.5|5/14/2014|40578|[web, twitter, FB...|       true|
    |  6|  Reynold|    Xin|https://tinyurl.6| 3/2/2015|25568| [twitter, LinkedIn]|       true|
    +---+---------+-------+-----------------+---------+-----+--------------------+-----------+
    """

    # * 세 컬럼을 연결하여 새로운 컬럼을 만들고 그 컬럼을 보여줌
    blogs_df.withColumn("AuthorsId", (concat(col("First"), col("Last"), col("Id")))).select(
        "AuthorsId"
    ).show()
    """
    +-------------+
    |    AuthorsId|
    +-------------+
    |  JulesDamji1|
    | BrookeWenig2|
    |    DennyLee3|
    |TathagataDas4|
    |MateiZaharia5|
    |  ReynoldXin6|
    +-------------+
    """

    # * 표현은 다르지만 동일한 결과
    blogs_df.select(expr("Hits")).show(2)
    blogs_df.select(col("Hits")).show(2)
    blogs_df.select("Hits").show(2)

    # * Id 컬럼값에 따라 역순 정렬
    blogs_df.sort(desc(col("Id"))).show()
    blogs_df.sort("Id", ascending=False).show()
