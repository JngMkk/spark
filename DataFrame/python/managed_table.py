"""
SQL 테이블과 뷰

Spark는 각 테이블과 해당 데이터에 관련된 정보인 스키마, 설명, 테이블명, 데이터베이스명, 컬럼명, 파티션, 실제 데이터의 물리적 위치 등 메타데이터를 가지고 있음.
이 모든 정보는 중앙 메타스토어에 저장됨.

Spark는 Spark 테이블만을 위한 별도 메타스토어를 생성하지 않고
기본적으로는 /user/hive/warehouse에 있는 아파치 하이브 메타스토어를 사용하여 테이블에 대한 모든 메타데이터를 유지함.
그러나 Spark 구성 변수 spark.sql.warehouse.dir을 로컬 또는 외부 분산 저장소로 설정하여 다른 위치로 기본 경로를 변경할 수 있음.


관리형 테이블과 비관리형 테이블

Spark는 관리형과 비관리형이라는 두 가지 유형의 테이블을 만들 수 있음.
관리형 테이블의 경우 Spark는 메타데이터와 파일 저장소의 데이터를 모두 관리함.
파일 저장소는 로컬 파일 시스템 또는 HDFS거나 Amazon S3 및 Azure Blob과 같은 객체 저장소일 수 있음.
비관리형 테이블의 경우에는 스파크는 오직 메타데이터만 관리하고 카산드라와 같은 외부 데이터 소스에서 데이터를 직접 관리함.

관리형 테이블을 사용하면 스파크는 모든 것을 관리하기 때문에 DROP TABLE과 같은 SQL 명령은 메타데이터와 실제 데이터를 모두 삭제함.
반면에, 비관리형 테이블의 경우에는 동일한 명령이 실제 데이터는 그대로 두고 메타데이터만 삭제하게 됨.
"""

from pyspark.sql import SparkSession

spark: SparkSession = (
    SparkSession.builder.config("spark.driver.host", "eddieK").appName("ManagedTable").getOrCreate()
)
spark.sql("CREATE DATABASE learn_spark_db")

# * 이 시점부터는 애플리케이션에서 실행되는 어떠한 명령어든 learn_spark_db 데이터베이스 안에서 생성되고 상주하게 됨.
spark.sql("USE learn_spark_db")

spark.sql(
    """
    CREATE TABLE managed_us_delay_flights_tbl (
        date        STRING,
        delay       INT,
        distance    INT,
        origin      STRING,
        destination STRING
    )
    """
)

file = "data/departuredelays.csv"
schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
flights_df = spark.read.csv(file, schema=schema)
flights_df.write.saveAsTable("managed_us_delay_flights_tbl")
