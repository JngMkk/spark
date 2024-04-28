from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType

# * Spark Data Frame API를 통한 스키마 정의
schema1 = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("author", StringType(), False),
        StructField("title", StringType(), False),
        StructField("pages", IntegerType(), False),
        StructField("etc", ArrayType(StringType(), False), False),
    ]
)

# * DDL을 통한 스키마 정의
schema2 = "id INT, author STRING, title STRING, pages INT, etc ARRAY<STRING>"
data = [[1, "Jules", "ABCD", 3, ["abc"]], [2, "Brooke", "EF", 1, ["abcd", "efg"]]]


if __name__ == "__main__":
    spark: SparkSession = SparkSession.builder.appName("SchemaEx").getOrCreate()

    df1 = spark.createDataFrame(data, schema=schema1)
    df2 = spark.createDataFrame(data, schema=schema2)
    df1.show()
    """
    +---+------+-----+-----+-----------+
    | id|author|title|pages|        etc|
    +---+------+-----+-----+-----------+
    |  1| Jules| ABCD|    3|      [abc]|
    |  2|Brooke|   EF|    1|[abcd, efg]|
    +---+------+-----+-----+-----------+
    """

    df2.show()
    """
    +---+------+-----+-----+-----------+
    | id|author|title|pages|        etc|
    +---+------+-----+-----+-----------+
    |  1| Jules| ABCD|    3|      [abc]|
    |  2|Brooke|   EF|    1|[abcd, efg]|
    +---+------+-----+-----+-----------+
    """
    df1.printSchema()
    """
    root
    |-- id: integer (nullable = false)
    |-- author: string (nullable = false)
    |-- title: string (nullable = false)
    |-- pages: integer (nullable = false)
    |-- etc: array (nullable = false)
    |    |-- element: string (containsNull = false)
    """

    df2.printSchema()
    """
    root
    |-- id: integer (nullable = true)
    |-- author: string (nullable = true)
    |-- title: string (nullable = true)
    |-- pages: integer (nullable = true)
    |-- etc: array (nullable = true)
    |    |-- element: string (containsNull = true)
    """

    # * 스키마를 코드의 다른 부분에서 사용하기 원한다면
    print(df1.schema)
    """
    StructType(
        [
            StructField('id', IntegerType(), False),
            StructField('author', StringType(), False),
            StructField('title', StringType(), False),
            StructField('pages', IntegerType(), False),
            StructField('etc', ArrayType(StringType(), False), False)
        ]
    )
    """
