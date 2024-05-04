"""
Pandas UDF로 pyspark UDF 속도 향상 및 배포

Pyspark UDF 사용과 관련해 기존의 일반적 문제 중 하나는 Scala UDF보다 성능이 느리다는 점.
이는 Pyspark UDF가 JVM과 Python 사이의 데이터 이동을 필요로 해서 비용이 많이 들었기 때문.
이 문제를 해결하기 위해 Pandas UDF(Vectorized UDF)가 Spark 2.3의 일부로 도입됨.

Pandas UDF는 Apache Arrow를 사용하여 데이터를 전송하고 Pandas는 해당 데이터로 작업을 함.
pandas_udf 키워드를 데코레이터로 사용하여 Pandas UDF를 정의하거나 함수 자체를 Wrapping할 수 있음.
Apache Arrow 형식에 포함된 데이터라면 이미 Python Process에서 사용할 수 있는 형식이므로 더 이상 데이터를 직렬화나 피클할 필요가 없음.
행마다 개별 입력에 대해 작업하는 대신 Pandas Series 또는 DataFrame에서 작업함.

Pandas UDF는 Python 3.6 이상 기반의 Spark 3.0에서 Pandas UDF와 Pandas 함수 API로 분할됨.

    Pandas UDF
        Spark 3.0에서 Pandas UDF는 pandas.Series, pandas.DataFrame, Tuple 및 Iterator와 같은 Python type hint로 Pandas UDF type을 유추함.
        이전에는 각 Pandas UDF type을 수동으로 정의하고 지정해야 했으나,
        현재 Pandas UDF에서는 시리즈와 시리즈, 시리즈 반복자와 시리즈 반복자, 다중 시리즈 반복자와 시리즈 반복자, 시리즈와 스칼라(단일값)를 Python type hint로 지원함.
    
    Pandas function API
        입력과 출력이 모두 Pandas 인스턴스인 Pyspark DataFrame에 로컬 파이썬 함수를 직접 적용할 수 있음.
        Spark 3.0의 경우 Pandas function API는 그룹화된 맵, 맵, 공동 그룹화된 맵을 지원함.

pip install pyarrow pandas
"""

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType

spark: SparkSession = (
    SparkSession.builder.config("spark.driver.host", "localhost")
    .appName("PandasUDFEx")
    .getOrCreate()
)


def cubed(s: pd.Series) -> pd.Series:
    return s * s * s


# * pandas udf 생성
cubed_udf = pandas_udf(cubed, returnType=LongType())

x = pd.Series([1, 2, 3])
print(cubed(x))
"""
0     1
1     8
2    27
dtype: int64
"""

df = spark.range(1, 4)
df.select("id", cubed_udf(col("id"))).show()
"""
+---+---------+
| id|cubed(id)|
+---+---------+
|  1|        1|
|  2|        8|
|  3|       27|
+---+---------+
"""
