import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, IntegerType, StringType}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.DataTypes

object DataFramAPISparkSQLEx {
    def main(args: Array[String]) = {
        // 해당 csv 파일에는 헤더가 있기 때문에 이를 사용해 암시적 스키마 검색을 이용해 데이터 프레임으로 빨리 로드할 수 있음.
        val statesDF = spark.read.option("header", "true").option("inferschema", "true").option("sep", ",").csv("statesPopulation.csv")
        // org.apache.spark.sql.DataFrame = [State: string, Year: int ... 1 more field]
        

        // 데이터 프레임 스키마 확인
        statesDF.printSchema
        // root
        // |-- State: string (nullable = true)
        // |-- Year: integer (nullable = true)
        // |-- Population: integer (nullable = true)


        // 데이터 프레임은 논리 계획을 파싱하고 해당 논리 계획을 분석하며
        // 플랜을 최적화한 후 마지막에 실제 실행 계획을 실행함으로써 동작
        statesDF.explain(true)
        /*
            == Parsed Logical Plan ==
            Relation [State#17,Year#18,Population#19] csv

            == Analyzed Logical Plan ==
            State: string, Year: int, Population: int
            Relation [State#17,Year#18,Population#19] csv

            == Optimized Logical Plan ==
            Relation [State#17,Year#18,Population#19] csv

            == Physical Plan ==
            FileScan csv [State#17,Year#18,Population#19] Batched: false, DataFilters: [], Format: CSV,
            Location: InMemoryFileIndex(1 paths)[file:/home/jngmk/workspace/scala/ch8/statesPopulation.csv],
            PartitionFilters: [], PushedFilters: [], ReadSchema: struct<State:string,Year:int,Population:int>
        */


        // 데이터 프레임은 테이블 이름으로 등록할 수 있음
        statesDF.createOrReplaceTempView("states")


        // 데이터 프레임이 구조화됐거나 테이블로 구성된다면 데이터를 조작하는 커맨드를 실행할 수 있음.
        statesDF.show(5)
        spark.sql("SELECT * FROM states LIMIT 5").show
        /*
        +----------+----+----------+
        |     State|Year|Population|
        +----------+----+----------+
        |   Alabama|2010|   4785492|
        |    Alaska|2010|    714031|
        |   Arizona|2010|   6408312|
        |  Arkansas|2010|   2921995|
        |California|2010|  37332685|
        +----------+----+----------+
        */

        // sort 연산을 사용하면 데이터 프레임의 로우를 임의의 칼럼으로 정렬할 수 있음.
        statesDF.sort(col("Population").desc).show(5)
        spark.sql("SELECT * FROM states ORDER BY population DESC LIMIT 5").show

        
        // groupby를 사용해 데이터 프레임을 특정 칼럼으로 그룹핑할 수 있음.
        statesDF.groupBy("State").sum("Population").show(5)
        spark.sql("SELECT state, SUM(population) FROM states GROUP BY state LIMIT 5").show


        // agg 연산을 사용하면 특정 칼럼의 최소, 최대, 평균을 찾는 것처럼 데이터 프레임의 칼럼에 대해 다양한 연산을 수행할 수 있음.
        statesDF.groupBy("State").agg(sum("Population").alias("total")).show(5)
        spark.sql("SELECT state, SUM(population) AS total FROM states GROUP BY state LIMIT 5").show


        // 자연스럽게 로직이 복잡해지면서 실행 계획도 복잡해짐
        statesDF.groupBy("State").agg(sum("Population").alias("total")).explain(true)
        /*
            == Parsed Logical Plan ==
            'Aggregate [State#17], [State#17, sum('Population) AS Total#184]
            +- Relation [State#17,Year#18,Population#19] csv

            == Analyzed Logical Plan ==
            State: string, Total: bigint
            Aggregate [State#17], [State#17, sum(Population#19) AS Total#184L]
            +- Relation [State#17,Year#18,Population#19] csv

            == Optimized Logical Plan ==
            Aggregate [State#17], [State#17, sum(Population#19) AS Total#184L]
            +- Project [State#17, Population#19]
                +- Relation [State#17,Year#18,Population#19] csv

            == Physical Plan ==
            AdaptiveSparkPlan isFinalPlan=false
            +- HashAggregate(keys=[State#17], functions=[sum(Population#19)], output=[State#17, Total#184L])
                +- Exchange hashpartitioning(State#17, 200), ENSURE_REQUIREMENTS, [id=#394]
                    +- HashAggregate(keys=[State#17], functions=[partial_sum(Population#19)], output=[State#17, sum#188L])
                        +- FileScan csv [State#17,Population#19] Batched: false, DataFilters: [], Format: CSV,
                        Location: InMemoryFileIndex(1 paths)[file:/home/jngmk/workspace/scala/ch8/statesPopulation.csv],
                        PartitionFilters: [], PushedFilters: [], ReadSchema: struct<State:string,Population:int>
        */


        // 데이터 프레임 연산을 함께 체이닝함으로써 실행 비용을 최적화할 수 있음
        // 텅스텐 성능 향상과 카탈리스트 옵티마이저가 함께 동작할 수 있음
        // 단일 구문으로 연산을 체이닝하는 예
        statesDF.groupBy("State").agg(sum("Population").alias("total")).sort(col("total").desc).show(5)
        spark.sql("SELECT state, SUM(population) AS total FROM states GROUP BY state ORDER BY total DESC LIMIT 5").show


        // 동시에 여러 집계 생성
        statesDF.groupBy("State").agg(
            min("Population").alias("minTotal"),
            max("Population").alias("maxTotal"),
            avg("Population").alias("avgTotal")).sort(col("minTotal").desc).show(5)

        spark.sql("""
        SELECT
            state,
            MIN(population) AS minTotal,
            MAX(population) AS maxTotal,
            AVG(population) AS avgTotal
        FROM states
        GROUP BY state
        ORDER BY minTotal DESC
        LIMIT 5
        """).show


        /*
            pivot
                여러 가지 요약과 집계를 수행하는 데 더 적합한 다른 뷰를 생성하기 위해 테이블을 변환하는 좋은 방법.
                pivot은 칼럼의 값을 얻어 각각의 값을 실제 칼럼으로 만들며 수행됨.
                각각의 유일한 값을 실제 칼럼으로 변환해 여러 개의 새로운 칼럼을 생성함.
        */
        statesDF.groupBy("State").pivot("Year").sum("Population").show(5)
        /*
        +---------+--------+--------+--------+--------+--------+--------+--------+
        |    State|    2010|    2011|    2012|    2013|    2014|    2015|    2016|
        +---------+--------+--------+--------+--------+--------+--------+--------+
        |     Utah| 2775326| 2816124| 2855782| 2902663| 2941836| 2990632| 3051217|
        |   Hawaii| 1363945| 1377864| 1391820| 1406481| 1416349| 1425157| 1428557|
        |Minnesota| 5311147| 5348562| 5380285| 5418521| 5453109| 5482435| 5519952|
        |     Ohio|11540983|11544824|11550839|11570022|11594408|11605090|11614373|
        | Arkansas| 2921995| 2939493| 2950685| 2958663| 2966912| 2977853| 2988248|
        +---------+--------+--------+--------+--------+--------+--------+--------+
        */


        /*
            filter
                새로운 데이터 프레임을 생성하기 위해 데이터 프레임 로우를 빨리 필터링할 수 있음.
                필터는 데이터 프레임을 좁힐 수 있는 데이터의 매우 중요한 변환을 가능케 함.
                따라서 연산의 성능을 향상시킬 수 있음.
        */
        statesDF.filter("State == 'California'").show
        statesDF.filter("State == 'California'").explain(true)
        /*
        == Parsed Logical Plan ==
        'Filter ('State = California)
        +- Relation [State#17,Year#18,Population#19] csv

        == Analyzed Logical Plan ==
        State: string, Year: int, Population: int
        Filter (State#17 = California)
        +- Relation [State#17,Year#18,Population#19] csv

        == Optimized Logical Plan ==
        Filter (isnotnull(State#17) AND (State#17 = California))
        +- Relation [State#17,Year#18,Population#19] csv

        == Physical Plan ==
        *(1) Filter (isnotnull(State#17) AND (State#17 = California))
        +- FileScan csv [State#17,Year#18,Population#19] Batched: false,
        DataFilters: [isnotnull(State#17), (State#17 = California)], Format: CSV,
        Location: InMemoryFileIndex(1 paths)[file:/home/jngmk/workspace/scala/ch8/statesPopulation.csv],
        PartitionFilters: [], PushedFilters: [IsNotNull(State), EqualTo(State,California)],
        ReadSchema: struct<State:string,Year:int,Population:int>
        */


        // 사용자 정의 함수(UDF)
        // UDF는 스파크 SQL의 기능을 확장하는 새로운 칼럼 기반 함수를 정의함
        // UDF는 내부적으로 case 사용자 정의 함수 클래스를 호출함. 해당 클래스는 ScalaUDF를 내부적으로 호출함.
        // import org.apache.spark.sql.functions._
        val toUpper: String => String =_.toUpperCase
        val toUpperUDF = udf(toUpper)   // org.apache.spark.sql.expressions.UserDefinedFunction = SparkUserDefinedFunction($Lambda$4811/0x0000000841944040@4ef105f1,StringType,List(Some(class[value[0]: string])),Some(class[value[0]: string]),None,true,true)

        statesDF.withColumn("StateUpperCase", toUpperUDF(col("State"))).show(5)
        /*
        +----------+----+----------+--------------+
        |     State|Year|Population|StateUpperCase|
        +----------+----+----------+--------------+
        |   Alabama|2010|   4785492|       ALABAMA|
        |    Alaska|2010|    714031|        ALASKA|
        |   Arizona|2010|   6408312|       ARIZONA|
        |  Arkansas|2010|   2921995|      ARKANSAS|
        |California|2010|  37332685|    CALIFORNIA|
        +----------+----+----------+--------------+
        */


        /*
            데이터의 스키마 구조
                스키마는 데이터 구조에 대한 설명이고 암시적이거나 명시적일 수 있음.

                데이터 프레임은 내부적으로 RDD를 기반으로 하기 때문에 기존 RDD를 데이터셋으로 변환하는 두 가지 주요 방법이 있음.
                RDD는 리플렉션을 사용해 RDD의 스키마를 추론해 데이터셋으로 변환될 수 있음.
                데이터셋을 생성하는 두 번째 방법은 프로그래밍 인터페이스를 통해 기존 RDD를 사용하고, RDD를 스키마가 있는 데이터셋으로 변환하는 스키마를 제공하는 것

                리플렉션을 사용해 스키마를 추론해 RDD에서 데이터 프레임을 생성할 수 있도록
                스파크의 스칼라 API는 테이블 스키마를 정의하는 데 사용할 수 있는 케이스 클래스를 제공함.
                케이스 클래스는 모든 경우에 사용하기가 쉽지 않기 때문에 RDD에서 데이터 프레임을 프로그래밍을 함으로써 생성함.

                - 암시 스키마
                    텍스트 파일에 헤더가 포함돼 있으면 읽기 API는 헤더 라인을 읽으며 스키마를 추론할 수 있음.
                    또한 텍스트 파일 라인을 분리하는 데 사용될 구분자를 지정할 수 있음.

                - 명시 스키마
                    SturctField 객체의 컬렉션인 StructType을 사용해 스키마를 표현할 수 있음.
                    import org.apache.spark.sql.types.{StructType, IntegerType, StringType}
        */

        val schema = new StructType().add("i", IntegerType).add("s", StringType)    // org.apache.spark.sql.types.StructType = StructType(StructField(i,IntegerType,true),StructField(s,StringType,true))

        schema.printTreeString
        /*
        root
        |-- i: integer (nullable = true)
        |-- s: string (nullable = true)
        */

        schema.prettyJson
        /*
        {
            "type" : "struct",
            "fields" : [
                {
                    "name" : "i",
                    "type" : "integer",
                    "nullable" : true,
                    "metadata" : { }
                },
                {
                    "name" : "s",
                    "type" : "string",
                    "nullable" : true,
                    "metadata" : { }
                }
            ]
        }
        */


        /*
            인코더
                스파크 2.x는 복잡한 데이터 타입에 대해 스키마를 정의하는 방법을 지원함
                
                import org.apache.spark.sql.Encoders
        */

        // 튜플을 데이터셋 API에서 사용할 데이터 타입으로 정의하는 간단한 예
        Encoders.product[(Integer, String)].schema.printTreeString
        /*
        root
        |-- _1: integer (nullable = true)
        |-- _2: string (nullable = true)
        */


        // 필요에 따라 케이스 클래스를 정의한 다음 사용할 수도 있음
        case class Record(i: Integer, s: String)
        Encoders.product[Record].schema.printTreeString

        // 배열이나 맵과 같은 복잡한 타입은 DataTypes 객체를 사용
        // import org.apache.spark.sql.types.DataTypes
        val arrayType = DataTypes.createArrayType(IntegerType)  // org.apache.spark.sql.types.ArrayType = ArrayType(IntegerType,true)


        /*
            스파크 SQL API에서 지원되는 데이터 타입
                데이터 타입         스칼라 값 타입                  데이터 타입에 접근하거나 생성할 수 있는 API
                ByteType            Byte                        ByteType
                ShortType           Short                       ShortType
                IntegerType         Int                         IntegerType
                LongType            Long                        LongType
                FloatType           Float                       FloatType
                DoubleType          Double                      DoubleType
                DecimalType         java.math.BigDecimal        DemicalType
                StringType          String                      StringType
                BinaryType          Array[Byte]                 BinaryType
                BooleanType         Boolean                     BooleanType
                TimestampType       java.sql.Timestamp          TimestampType
                DateType            java.sql.Date               DateType
                ArrayType           scala.collection.Seq        ArrayType(elementType, [containsNull])
                MapType             scala.collection.Map        MapType(keyType, valueType, [valueContainsNull]) valueContainsNull 디폴트 값은 true
                StructType          org.apache.spark.sql.Row    StructType(fields)  Seq의 필드는 StructFields임. 또한 동일한 이름을 갖는 두 개의 필드를 허용하지 않음.
        */


        /*
            데이터셋 로드와 저장
                실제 작업을 진행하려면 입출력 또는 결과를 저장소에 저장하고 클러스터에서 데이터를 다시 읽게 해야 함.
                입력 데이터는 다양한 데이터셋이나 파일, 아마존 S3 저장소, 데이터베이스, NoSQL, 하이브 같은 소스에서 읽을 수 있고,
                출력도 비슷하게 파일, S3, 데이터베이스, 하이브 등에 저장할 수 있음.

                해당 소스 중 일부는 커넥터를 통해 스파크를 지원함.
        */

        // 데이터셋 로드 (Parquet, csv, Hive table, jdbc, orc, text, json)
        val statesPopDF = spark.read.option("header", "true").option("inferschema", "true").option("sep", ",").csv("statesPopulation.csv")

        // 데이터셋 저장
        statesPopDF.write.option("header", "true").csv("statesPopulation_dup.csv")
    }
}