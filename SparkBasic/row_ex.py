from pyspark.sql import Row, SparkSession

spark: SparkSession = SparkSession.builder.appName("RowEx").getOrCreate()
row = Row(
    1, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015", ["twitter", "LinkedIn"]
)  # type:ignore
print(row[1])
"Reynold"

# * Row 객체들은 빠른 탐색을 위해 데이터 프레임으로 만들어 사용하기도 함.
rows = [Row("Matei Zaharia", "CA"), Row("Reynold Xin", "CA")]
authors_df = spark.createDataFrame(rows, ["Authors", "State"])
authors_df.show()
"""
+-------------+-----+
|      Authors|State|
+-------------+-----+
|Matei Zaharia|   CA|
|  Reynold Xin|   CA|
+-------------+-----+
"""
