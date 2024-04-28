import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark: SparkSession = (
    SparkSession.builder.config("spark.driver.host", "localhost")
    .appName("DataFrameReaderWriterEx")
    .getOrCreate()
)

# * spark.read.csv(): csv 파일을 읽어서 row 객체와 스키마에 맞는 타입의 이름 있는 컬럼들로 이루어진 데이터 프레임을 되돌려 줌.
# * 스키마를 미리 지정하고 싶지 않다면, 스파크가 적은 비용으로 샘플링해서 스키마를 추론할 수 있게 할 수 있음.
sf_fire_df = (
    spark.read.option("samplingRatio", 0.001).option("header", True).csv("data/sf-fire-calls.csv")
)
print(sf_fire_df.schema)

few_fire_df = sf_fire_df.select("IncidentNumber", "AvailableDtTm", "CallType").where(
    F.col("CallType") != "Medical Incident"
)
few_fire_df.show(5, truncate=False)
"""
+--------------+----------------------+--------------+
|IncidentNumber|AvailableDtTm         |CallType      |
+--------------+----------------------+--------------+
|2003235       |01/11/2002 01:51:44 AM|Structure Fire|
|2003250       |01/11/2002 04:16:46 AM|Vehicle Fire  |
|2003259       |01/11/2002 06:01:58 AM|Alarms        |
|2003279       |01/11/2002 08:03:26 AM|Structure Fire|
|2003301       |01/11/2002 09:46:44 AM|Alarms        |
+--------------+----------------------+--------------+
"""

# * 화재 신고로 기록된 CallType 종류가 몇 가지인지 알고 싶다면?
# * count_distinct(): 컬럼의 도메인 개수를 리턴
sf_fire_df.select("CallType").where(F.col("CallType").isNotNull()).agg(
    F.count_distinct("CallType").alias("DistinctCallTypes")
).show()
"""
+-----------------+
|DistinctCallTypes|
+-----------------+
|               30|
+-----------------+
"""

# * Null이 아닌 신고 타입의 목록 추출
sf_fire_df.select("CallType").where(F.col("CallType").isNotNull()).distinct().show(10, False)
"""
+-----------------------------+
|CallType                     |
+-----------------------------+
|Elevator / Escalator Rescue  |
|Marine Fire                  |
|Aircraft Emergency           |
|Administrative               |
|Alarms                       |
|Odor (Strange / Unknown)     |
|Citizen Assist / Service Call|
|HazMat                       |
|Watercraft in Distress       |
|Explosion                    |
+-----------------------------+
only showing top 10 rows
"""

# * withColumnRenamed(): 원하는 컬럼 이름 변경
# * 데이터 프레임 변형은 변경 불가 방식으로 동작하므로 컬럼 이름을 변경할 때는 원본을 유지한 채로 컬럼 이름이 변경된 새로운 데이터 프레임을 반환함.
delayed_fire_df = sf_fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
delayed_fire_df.select("ResponseDelayedinMins").where(F.col("ResponseDelayedinMins") > 5).show(
    5, False
)
"""
+---------------------+
|ResponseDelayedinMins|
+---------------------+
|6.25                 |
|7.25                 |
|11.916667            |
|8.633333             |
|95.28333             |
+---------------------+
only showing top 5 rows
"""

# * StringType to Date or TimeStamp
# * to_timestamp() / to_date()
# * drop(): 컬럼 삭제
fire_ts_df = (
    sf_fire_df.withColumn("IncidentDate", F.to_timestamp(F.col("CallDate"), "MM/dd/yyyy"))
    # .drop("CallDate")
    .withColumn("OnWatchDate", F.to_timestamp(F.col("WatchDate"), "MM/dd/yyyy"))
    # .drop("WatchDate")
    .withColumn("AvailableDtTS", F.to_timestamp(F.col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
    # .drop("AvailableDtTm")
)
fire_ts_df.select("IncidentDate", "OnWatchDate", "AvailableDtTS").show(5, False)
"""
+-------------------+-------------------+-------------------+
|IncidentDate       |OnWatchDate        |AvailableDtTS      |
+-------------------+-------------------+-------------------+
|2002-01-11 00:00:00|2002-01-10 00:00:00|2002-01-11 01:51:44|
|2002-01-11 00:00:00|2002-01-10 00:00:00|2002-01-11 03:01:18|
|2002-01-11 00:00:00|2002-01-10 00:00:00|2002-01-11 02:39:50|
|2002-01-11 00:00:00|2002-01-10 00:00:00|2002-01-11 04:16:46|
|2002-01-11 00:00:00|2002-01-10 00:00:00|2002-01-11 06:01:58|
+-------------------+-------------------+-------------------+
only showing top 5 rows

AvailableDtTm에서 마지막에 a를 붙히지 않으면 Spark 버전 3.0부터 날짜 및 시간 문자열을 구문 분석하는 방식 변경 에러가 발생함.
제공된 형식 MM/dd/yyyy hh:mm:ss이 12시간제 형식의 사용을 올바르게 지정하지 않았기 때문에 문자열 01/11/2002 01:51:44 AM 같은 문자열을 구문 분석하지 못함.

또는 여러 위치에서 문제가 발생하고 변경 전 파서의 동작을 유지하려는 경우 Spark Session에서 레거시 시간 Parser 정책을 설정할 수 있음.
    => spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
"""

# * 수정된 날짜/시간 컬럼으로 데이터 탐색을 할 때 dayofmonth(), dayofyear(), dayofweek() 같은 함수를 통해 쿼리할 수 있음.
fire_ts_df.select(F.year("IncidentDate")).distinct().orderBy(F.year("IncidentDate")).show()
"""
+------------------+
|year(IncidentDate)|
+------------------+
|              2000|
|              2001|
|              2002|
|              2003|
|              2004|
|              2005|
|              2006|
|              2007|
|              2008|
|              2009|
|              2010|
|              2011|
|              2012|
|              2013|
|              2014|
|              2015|
|              2016|
|              2017|
|              2018|
+------------------+
"""

# =========================================================================================
# * 집계

# * 가장 흔한 형태의 신고는?
# * COUNT(col_name): null 포함 x, COUNT(*) / COUNT(1): null 포함
# * groupby(): null 값을 그룹화 대상으로 포함하지 않음.
sf_fire_df.select("CallType").groupBy("CallType").count().orderBy("count", ascending=False).show(
    10, False
)
"""
+-------------------------------+------+
|CallType                       |count |
+-------------------------------+------+
|Medical Incident               |113794|
|Structure Fire                 |23319 |
|Alarms                         |19406 |
|Traffic Collision              |7013  |
|Citizen Assist / Service Call  |2524  |
|Other                          |2166  |
|Outside Fire                   |2094  |
|Vehicle Fire                   |854   |
|Gas Leak (Natural and LP Gases)|764   |
|Water Rescue                   |755   |
+-------------------------------+------+
only showing top 10 rows
"""

# * 데이터 프레임 API는 collect() 메서드를 제공하지만 극단적으로 큰 데이터 프레임에서는 메모리 부족 예외를 발생시킬 수 있기 때문에 위험함.
# * 드라이버에 숫자 하나만 전달하는 count()와 달리 collect()는 전체 데이터 프레임 혹은 데이터세트의 모든 Row 객체 모음을 되돌려 줌.
# * 몇 개의 Row 결과만 보고 싶다면 최초 n개의 Row 객체만 되돌려 주는 take(n) 함수를 쓰는 것이 훨씬 나음.
# print(
#     sf_fire_df.select("CallType")
#     .groupBy("CallType")
#     .count()
#     .sort("count", ascending=False)
#     .collect()
# )
"""
[
    Row(CallType="Medical Incident", count=113794),
    Row(CallType="Structure Fire", count=23319),
    Row(CallType="Alarms", count=19406),
    Row(CallType="Traffic Collision", count=7013),
    Row(CallType="Citizen Assist / Service Call", count=2524),
    Row(CallType="Other", count=2166),
    Row(CallType="Outside Fire", count=2094),
    Row(CallType="Vehicle Fire", count=854),
    Row(CallType="Gas Leak (Natural and LP Gases)", count=764),
    Row(CallType="Water Rescue", count=755),
    Row(CallType="Odor (Strange / Unknown)", count=490),
    Row(CallType="Electrical Hazard", count=482),
    Row(CallType="Elevator / Escalator Rescue", count=453),
    Row(CallType="Smoke Investigation (Outside)", count=391),
    Row(CallType="Fuel Spill", count=193),
    Row(CallType="HazMat", count=124),
    Row(CallType="Industrial Accidents", count=94),
    Row(CallType="Explosion", count=89),
    Row(CallType="Train / Rail Incident", count=57),
    Row(CallType="Aircraft Emergency", count=36),
    Row(CallType="Assist Police", count=35),
    Row(CallType="High Angle Rescue", count=32),
    Row(CallType="Watercraft in Distress", count=28),
    Row(CallType="Extrication / Entrapped (Machinery, Vehicle)", count=28),
    Row(CallType="Oil Spill", count=21),
    Row(CallType="Suspicious Package", count=15),
    Row(CallType="Marine Fire", count=14),
    Row(CallType="Confined Space / Structure Collapse", count=13),
    Row(CallType="Mutual Aid / Assist Outside Agency", count=9),
    Row(CallType="Administrative", count=3),
]
"""

# print(
#     sf_fire_df.select("CallType").groupBy("CallType").count().sort("count", ascending=False).take(5)
# )
"""
[
    Row(CallType='Medical Incident', count=113794),
    Row(CallType='Structure Fire', count=23319),
    Row(CallType='Alarms', count=19406),
    Row(CallType='Traffic Collision', count=7013),
    Row(CallType='Citizen Assist / Service Call', count=2524)
]
"""

# * 데이터 과학 작업에서 일반적으로 쓰이는 좀 더 고수준의 요구사항을 만족하려면 stat(), describe(), correlation(), sampleBy(), approxQuantile(), frequentItems() 등을 사용.
agg_df = sf_fire_df.select(F.sum("NumAlarms"), F.avg("Delay"), F.min("Delay"), F.max("Delay"))
agg_df.show()
"""
+--------------+------------------+-----------+----------+
|sum(NumAlarms)|        avg(Delay)| min(Delay)|max(Delay)|
+--------------+------------------+-----------+----------+
|      176170.0|3.8923641541750134|0.016666668|      99.9|
+--------------+------------------+-----------+----------+
"""

# * 데이터 프레임을 외부 데이터 소스에 원하는 포맷으로 쓰려면 DataFrameWriter 인터페이스를 사용할 수 있음.
# * DataFrameReader와 마찬가지로 다양한 데이터 소스를 지원함.
# * 기본 포맷은 컬럼 지향 포맷인 Parquet이며 데이터 압축에 snappy 압축을 사용함.
# * 만약 데이터 프레임이 파케이로 쓰여졌다면 스키마는 파케이 메타데이터의 일부로 보존될 수 있음.
agg_df.write.format("parquet").save("data/sf-fire.parquet")

spark.stop()
