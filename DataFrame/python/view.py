"""
Spark는 기존 테이블을 토대로 뷰를 만들 수 있음.
뷰는 전역(해당 클러스터의 모든 SparkSession에서 볼 수 있는) 또는 세션 범위(단일 SparkSession에서만 볼 수 있는)일 수 있으며 Spark 애플리케이션이 종료되면 사라짐.
뷰는 테이블과 달리 실제로 데이터를 소유하지 않기 때문에 애플리케이션이 종료되면 테이블은 유지되지만 뷰는 사라짐.

단일 Spark 애플리케이션 내에서 여러 SparkSession을 만들 수 있음.
이러한 경우를 예를 들면 동일한 하이브 메타스토어 구성을 공유하지 않는 두 개의 서로 다른 SparkSession에서 같은 데이터에 액세스하고 결합하고자 할 때 Global View가 유용.
"""

from pyspark.sql import SparkSession

spark: SparkSession = (
    SparkSession.builder.config("spark.driver.host", "eddieK").appName("ViewEx").getOrCreate()
)

file = "data/departuredelays.csv"
schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
df = spark.read.csv(file, schema=schema)
df.createOrReplaceTempView("us_delay_flights_tbl")

df_sfo = spark.sql(
    """
    SELECT
        date,
        delay,
        origin,
        destination
    FROM
        us_delay_flights_tbl
    WHERE
        origin = 'SFO'
    """
)

df_jfk = spark.sql(
    """
    SELECT
        date,
        delay,
        origin,
        destination
    FROM
        us_delay_flights_tbl
    WHERE
        origin = 'JFK'
    """
)


# * Global View
df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")

# * Temp view
df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")

# * 뷰를 생성한 후에는 테이블에 대해 수행하는 것처럼 쿼리를 실행할 수 있음.
# * Spark는 global_temp라는 전역 임시 데이터베이스에 전역 임시 뷰를 생성하므로 해당 뷰에 액세스할 때는 global_temp.<view_name>을 사용해야 함.
spark.read.table("global_temp.us_origin_airport_SFO_global_tmp_view").show(10)
# spark.read.table("us_origin_airport_JFK_tmp_view")
spark.sql("SELECT * FROM us_origin_airport_JFK_tmp_view").show(10)

"""
Spark는 각 관리형 및 비관리형 테이블에 대한 메타데이터를 관리함.
이는 메타데이터 저장을 위한 스파크 SQL의 상위 추상화 모듈인 카탈로그에 저장됨.
이 카탈로그는 Spark 2.x에서 새롭게 확장된 기능으로 데이터베이스, 테이블 및 뷰와 관련된 메타데이터를 검사함.
Spark 3.0은 외부 카탈로그를 사용하도록 개선됨.
"""
print(spark.catalog.listDatabases())
print(spark.catalog.listTables())
print(spark.catalog.listColumns("us_origin_airport_JFK_tmp_view"))

spark.catalog.dropGlobalTempView("us_origin_airport_SFO_global_tmp_view")
spark.catalog.dropTempView("us_origin_airport_JFK_tmp_view")
