import sys

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)

spark: SparkSession = (
    SparkSession.builder.config("spark.driver.host", "localhost").appName("MnMCount").getOrCreate()
)

mnm_file = sys.argv[-1]

# * inferSchema: 스키마 추론
# * header: 칼럼 이름이 제공됨
# * csv 포맷을 읽어 들여 데이터 프레임에 저장함
mnm_df = (
    spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(mnm_file)
)

count_mnm_df = (
    mnm_df.select("State", "Color", "Count")
    .groupBy("State", "Color")
    .agg(F.sum("Count").alias("Total"))  # 각 주 및 색깔별로 Count를 합침
    .orderBy(F.desc("Total"))
)
count_mnm_df.explain(True)
"""
== Parsed Logical Plan ==
'Sort ['Total DESC NULLS LAST], true
+- Aggregate [State#17, Color#18], [State#17, Color#18, sum(Count#19) AS Total#31L]
   +- Project [State#17, Color#18, Count#19]
      +- Relation [State#17,Color#18,Count#19] csv

== Analyzed Logical Plan ==
State: string, Color: string, Total: bigint
Sort [Total#31L DESC NULLS LAST], true
+- Aggregate [State#17, Color#18], [State#17, Color#18, sum(Count#19) AS Total#31L]
   +- Project [State#17, Color#18, Count#19]
      +- Relation [State#17,Color#18,Count#19] csv

== Optimized Logical Plan ==
Sort [Total#31L DESC NULLS LAST], true
+- Aggregate [State#17, Color#18], [State#17, Color#18, sum(Count#19) AS Total#31L]
   +- Relation [State#17,Color#18,Count#19] csv

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [Total#31L DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(Total#31L DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=37]
      +- HashAggregate(keys=[State#17, Color#18], functions=[sum(Count#19)], output=[State#17, Color#18, Total#31L])
         +- Exchange hashpartitioning(State#17, Color#18, 200), ENSURE_REQUIREMENTS, [plan_id=34]
            +- HashAggregate(keys=[State#17, Color#18], functions=[partial_sum(Count#19)], output=[State#17, Color#18, sum#36L])
               +- FileScan csv [State#17,Color#18,Count#19] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/eddiek/workspace/spark-test/data/mnm_dataset.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<State:string,Color:string,Count:int>
"""

# * show()는 action이므로 위의 쿼리 내용들이 평가됨.
count_mnm_df.show(n=60, truncate=False)
print(f"Total Rows = {count_mnm_df.count()}")

# * California에 대해 보기
ca_count_mnm_df = (
    mnm_df.select("State", "Color", "Count")
    .where("State = 'CA'")
    .groupBy("State", "Color")
    .sum("Count")
    .orderBy(F.desc("sum(Count)"))
)
ca_count_mnm_df.show(n=10, truncate=False)

# * SparkSession stop
spark.stop()
