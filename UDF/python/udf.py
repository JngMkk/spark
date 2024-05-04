"""
UDF (user-defined function) 사용자 정의 함수

udf는 세션별로 작동하며 기본 메타 스토어에서는 유지되지 않음.

스파크 SQL(SQL, DataFrame, Dataset)은 하위 표현식의 평가 순서를 보장하지 않음.
예를 들어, SELECT s FROM test WHERE s IS NOT NULL AND strlen(s) > 1이라는 쿼리가 있을 때,
s IS NOT NULL절이 strlen(s) > 1 절 이전에 실행된다는 것을 보장하지 않음.
따라서, 적절한 null 검사를 수행하려면 UDF 자체가 null을 인식하도록 만들고 UDF 내부에서 null 검사를 수행함.
혹은, IF 또는 CASE WHEN 식을 사용하여 null 검사를 수행하고 조건 분기에서 UDF를 호출함.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import LongType

spark: SparkSession = (
    SparkSession.builder.config("spark.driver.host", "localhost").appName("UDFEx").getOrCreate()
)


def cubed(s):
    return s * s * s


# * UDF 등록
spark.udf.register("cubed", cubed, LongType())

# * 임시 뷰 생성
spark.range(1, 9).createOrReplaceTempView("udf_test")

spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()

"""
+---+--------+                                                                  
| id|id_cubed|
+---+--------+
|  1|       1|
|  2|       8|
|  3|      27|
|  4|      64|
|  5|     125|
|  6|     216|
|  7|     343|
|  8|     512|
+---+--------+
"""
