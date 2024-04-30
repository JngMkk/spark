import org.apache.spark.sql.SparkSession

import java.util.Properties
import scala.language.postfixOps

/*
RDD에서 DataFrame을 생성하는 방법
    - 로우 데이터를 튜플 형태로 저장한 RDD를 사용하는 방법
        가장 기초적이고 간단하지만 스키마 속성을 전혀 지정할 수 없기 때문에 다소 제한적.

    - 케이스 클래스를 사용하는 방법
        케이스 클래스를 작성하는 것으로 조금 더 복잡하지만, 첫 번째 방법만큼 제한적이지 않음.

    - 스키마를 명시적을 지정하는 방법
        많이 사용.
 */

object DataFrameEx {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
        val sc = spark.sparkContext
        import spark.implicits._

        // commentCount, lastActivityDate, ownerUserID, body, score, creationDate, viewCount,
        // title, tags, answerCount, acceptedAnswerId, postTypeId, id
        val itPostsRows = sc.textFile("italianPosts.csv")
        val itPostsSplit = itPostsRows.map(x => x.split("~"))

        // 튜플 형식의 RDD에서 DataFrame 생성
        val itPostsRDD = itPostsSplit.map(x => (x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12)))
        val itPostsDFrame = itPostsRDD.toDF()
        itPostsDFrame.show(5)

        val itPostsDF = itPostsRDD.toDF("commetCount", "lastActivityDate", "ownerUserID", "body", "score",
            "creationDate", "viewCount", "title", "tags", "answerCount", "acceptedAnswerId", "postTypeId", "id")
        itPostsDF.show(5)
        itPostsDF.printSchema

        // 케이스 클래스를 사용해 RDD를 DataFrame으로 변환
        import java.sql.Timestamp

        case class Post(
                       commentCount: Option[Int],
                       lastActivityDate: Option[Timestamp],
                       ownerUserId: Option[Long],
                       body: String,
                       score: Option[Int],
                       creationDate: Option[Timestamp],
                       viewCount: Option[Int],
                       title: String,
                       tags: String,
                       answerCount: Option[Int],
                       acceptedAnswerId: Option[Long],
                       postTypeId: Option[Long],
                       id: Long
                       )

        // itPostsRDD를 Post 객체로 매핑하기 전에 다음과 같이 암시적 클래스를 정의하면 코드를 더욱 깔끔하게 작성할 수 있음.
        // 문자열을 각 타입으로 변환할 수 없을 때 예외를 던지는 대신 None으로 반환.
        // catching 함수는 scala.util.control.Exception.Catch 타입의 객체를 반환함.
        // 이 객체의 opt 메서드는 사용자가 지정한 함수 결과를 Option 객체로 매핑함.
        object StringImplicits {
            implicit class StringImprovements(val s: String) {
                import scala.util.control.Exception.catching
                def toIntSafe: Option[Int] = catching(classOf[NumberFormatException]) opt s.toInt
                def toLongSafe: Option[Long] = catching(classOf[NumberFormatException]) opt s.toLong
                def toTimestampSafe: Option[Timestamp] = catching(classOf[IllegalArgumentException]) opt Timestamp.valueOf(s)
            }
        }

        import StringImplicits._
        def stringToPost(row: String): Post = {
            val r = row.split("~")
            Post(r(0).toIntSafe,
                r(1).toTimestampSafe,
                r(2).toLongSafe,
                r(3),
                r(4).toIntSafe,
                r(5).toTimestampSafe,
                r(6).toIntSafe,
                r(7),
                r(8),
                r(9).toIntSafe,
                r(10).toLongSafe,
                r(11).toLongSafe,
                r(12).toLong
            )
        }

        val itPostsDFCase = itPostsRows.map(x => stringToPost(x)).toDF()
        itPostsDFCase.printSchema()
        /*
            root
             |-- commentCount: integer (nullable = true)
             |-- lastActivityDate: timestamp (nullable = true)
             |-- ownerUserId: long (nullable = true)
             |-- body: string (nullable = true)
             |-- score: integer (nullable = true)
             |-- creationDate: timestamp (nullable = true)
             |-- viewCount: integer (nullable = true)
             |-- title: string (nullable = true)
             |-- tags: string (nullable = true)
             |-- answerCount: integer (nullable = true)
             |-- acceptedAnswerId: long (nullable = true)
             |-- postTypeId: long (nullable = true)
             |-- id: long (nullable = false)
         */

        // 스키마를 지정해 RDD를 DataFrame으로 변환
        // createDataFrame 메서드 사용. Row 타입의 객체를 포함하는 RDD와 StructType 객체를 인자로 전달
        // StructType은 스파크 SQL의 테이블 스키마를 표현하는 클래스.
        // StructType 객체는 테이블 칼럼을 표현하는 StructField 객체를 한 개 또는 여러 개 가질 수 있음.
        import org.apache.spark.sql.types._
        import org.apache.spark.sql.Row

        val postSchema = StructType(Seq(
            StructField("commentCount", IntegerType, nullable = true),
            StructField("lastActivityDate", TimestampType, nullable = true),
            StructField("ownerUserId", LongType, nullable = true),
            StructField("body", StringType, nullable = true),
            StructField("score", IntegerType, nullable = true),
            StructField("creationDate", TimestampType, nullable = true),
            StructField("viewCount", IntegerType, nullable = true),
            StructField("title", StringType, nullable = true),
            StructField("tags", StringType, nullable = true),
            StructField("answerCount", IntegerType, nullable = true),
            StructField("acceptedAnswerId", LongType, nullable = true),
            StructField("postTypeId", LongType, nullable = true),
            StructField("id", LongType, nullable = false)
        ))

        def stringToRow(row: String): Row = {
            val r = row.split("~")
            Row(r(0).toIntSafe.orNull,
                r(1).toTimestampSafe.orNull,
                r(2).toLongSafe.orNull,
                r(3),
                r(4).toIntSafe.orNull,
                r(5).toTimestampSafe.orNull,
                r(6).toIntSafe.orNull,
                r(7),
                r(8),
                r(9).toIntSafe.orNull,
                r(10).toLongSafe.orNull,
                r(11).toLongSafe.orNull,
                r(12).toLong
            )
        }

        val rowRDD = itPostsRows.map(row => stringToRow(row))
        val itPostsDFStruct = spark.createDataFrame(rowRDD, postSchema)
        itPostsDFStruct.printSchema()

        /*
        DataFrame이 지원하는 데이터 타입
            대부분의 관계형 데이터베이스에서 일반적으로 지원하는 String, integer, float, double, byte, Date,
            Timestamp, Binary(관계형 데이터베이스에서는 BLOB) 타입뿐만 아니라 배열, 맵, 구조체 등 복합 데이터 타입도 지원.

            배열 : 동일한 타입의 여러 값을 포함
            맵 : 복수의 키-값 쌍을 포함. 단 키는 primitive 타입
            StructType : 중첩된 하위 컬럼 구조를 정의할 수 있음.
         */

        itPostsDFCase.columns   // Array[String] = Array(commentCount, lastActivityDate, ownerUserId, body, score, creationDate, viewCount, title, tags, answerCount, acceptedAnswerId, postTypeId, id)
        itPostsDFStruct.dtypes  // Array[(String, String)] = Array((commentCount,IntegerType), (lastActivityDate,TimestampType), (ownerUserId,LongType), (body,StringType), (score,IntegerType), (creationDate,TimestampType), (viewCount,IntegerType), (title,StringType), (tags,StringType), (answerCount,IntegerType), (acceptedAnswerId,LongType), (postTypeId,LongType), (id,LongType))

        // select
        import org.apache.spark.sql.functions._
        val postDF = itPostsDFStruct
        postDF.select("id", "body").show(3)
        postDF.select(col("id"), col("body")).show(3)
        postDF.select(postDF.col("id"), postDF.col("body")).show(3)
        postDF.select(Symbol("id"), Symbol("body")).show(3)
        postDF.select('id, 'body).show(3)
        val pIdBody = postDF.select($"id", $"body")

        // drop : 지정한 칼럼을 제외한 나머지 칼럼을 선택
        pIdBody.drop("id").show(3)

        // Data Filtering : filter, where
        pIdBody.filter($"body" contains "Italiano").count
        postDF.filter(($"postTypeId" === 1) and ($"acceptedAnswerId" isNull))
        postDF.filter('postTypeid === 1).limit(10).show

        // 칼럼을 추가하거나 칼럼 이름 변경
        pIdBody.withColumnRenamed("body", "mom").show(3)
        postDF.filter('postTypeid === 1).withColumn("ratio", $"viewCount" / $"score").where($"ratio" < 35).show(3)

        // 데이터 연산
        // 수학 계산 : abs, hypot(칼럼 값이나 스칼라 값 두 개를 각각 삼각형의 밑변과 높이로 가정하고, 이 삼각형의 빗변 길이를 계산), log, cbrt(세제곱근)
        // 문자열 연산 : length, trim, concat
        // 날짜 및 시간 연산 : year(날짜 컬럼의 연도를 반환), date_add(날짜 컬럼에 입력된 일수만큼 더함)
        postDF.filter($"postTypeId" === 1).
            withColumn("activePeriod", datediff($"lastActivityDate", $"creationDate")).
            orderBy($"activePeriod" desc).head.getString(3).replace("&lt;", "<").replace("&gt;", ">")

        postDF.select(avg($"score"), max($"score"), count($"score")).show

        /*
        윈도 함수
            집계 함수와 유사하지만, 로우들을 단일 결과로만 그룹핑하지 않는다는 점이 다름.
            윈도 함수에는 움직이는 그룹, 즉 프레임을 정의할 수 있음.
            프레임은 윈도 함수가 현재 처리하는 로우와 관련된 다른 로우 집합으로 정의하며, 이 집합을 현재 로우 계산에 활용할 수 있음.
            예를 들어 윈도 함수로 이동 평균이나 누적 합 등을 계산할 수 있음.
            이 값들을 계산하려면 보통 서브 select 쿼리나 복잡한 조인 연산이 필요하지만, 윈도 함수를 사용해 훨씬 더 간단하고 단순하게 계산할 수 있음.

            윈도 함수를 사용하려면 먼저 집계 함수나 first, ntile, rank 등의 함수 중 하나를 사용해 Column 정의를 구성해야 함.
            그다음 윈도 객체를 생성하고, 이를 Column의 over 함수에 인수로 전달함.
            over 함수는 이 윈도 객체를 사용하는 윈도 컬럼을 정의해 반환함.

            first(column)
                프레임에 포함된 로우 중에서 첫 번째 로우 값 반환
            last(column)
                프레임에 포함된 로우 중에서 마지막 로우 값 반환
            lag(column, offset, [default])
                프레임에 포함된 로우 중에서 현재 로우를 기준으로 offset만큼 뒤에 있는 로우 값을 반환함.
                이 위치에 로우가 없다면 주어진 default 값을 사용
            lead(column, offset, [default])
                프레임에 포함된 로우 중에서 현재 로우를 기준으로 offset만큼 앞에 있는 로우 값을 반환함.
                이 위치에 로우가 없다면 주어진 default 값을 사용
            ntile(n)
                프레임에 포함된 로우를 그룹 n개로 분할하고, 현재 로우가 속한 그룹의 인덱스를 반환함.
                프레임에 포함된 로우 개수가 n으로 나누어 떨어지지 않고 몫이 임의의 정수 x와 x+1 사이의 실수일 때,
                각 그룹은 로우 x개 또는 x+1개를 각각 나누어 가짐. 이때 순서가 빠른 그룹에 로우 x+1개를 먼저 할당함.
            cume_dist
                프레임에 포함된 로우 중에서 현재 처리하고 있는 로우의 값보다 작거나 같은 다른 로우들이 프레임에서 차지하는 비율을 반환함.
            rank
                프레임에 포함된 로우 중에서 현재 처리하고 있는 로우 순위를 반환함. 로우 순위는 특정 컬럼 값을 기준으로 계산.
            dense_rank
                프레임에 포함된 로우 중에서 현재 처리하고 있는 로우 순위를 반환함.
                이 함수가 rank 함수와 다른 점은 값이 동일한 로우에 동일한 순위를 부여하고, 다음 순위로 바로 이어짐.
                예를 들어 로우 세 개가 동일하게 1위 값을 가질 경우, 세 로우 모두에 1위를 부여하고 그 다음 네 번째 로우에는 2위를 부여함.
                rank는 네 번째 로우에 4위를 부여함.
            percent_rank
                프레임에 포함된 로우 중에서 현재 처리하고 있는 로우 순위를 프레임의 전체 로우 개수로 나누어 반환.
            row_number
                프레임에 포함된 로우 중에서 현재 처리하고 있는 로우 순번을 반환함.
         */
        import org.apache.spark.sql.expressions.Window

        postDF.filter($"postTypeId" === 1).
            select($"ownerUserId", $"acceptedAnswerId", $"score", max($"score").
            over(Window.partitionBy($"ownerUserId")) as "maxPerUser").
            withColumn("toMax", $"maxPerUser" - $"score").show(10)

        val winFunc = Window.partitionBy($"ownerUserId").orderBy($"creationDate")
        postDF.filter($"postTypeId" === 1).
            select($"ownerUserId", $"id", $"creationDate",
                lag($"id", 1).over(winFunc).alias("prev"),
                lead($"id", 1).over(winFunc).alias("next")
            ).show(10)

        // 사용자 정의 함수
        // String 객체에 암시적으로 추가되는 r 메서드를 호출하면 해당 문자열의 Regex 클래스를 생성할 수 있음.
        // Regex 클래스는 findAllMatchIn 메서드 외에도 split, replace, unapplySeq 메서드를 제공함
        val countTags = udf((tags: String) => "&lt;".r.findAllMatchIn(tags).length)
        val countTags2 = spark.udf.register("countTags", (tags: String) => "&lt;".r.findAllMatchIn(tags).length)
        postDF.filter($"postTypeId" === 1).
            select($"tags", countTags($"tags") as "tagCnt").show(10, truncate = false)
        postDF.filter($"postTypeId" === 1).
            select($"tags", countTags2($"tags") as "tagCnt").show(10, truncate = false)

        /*
        결측 값 다루기
            데이터를 사용하기 전에 데이터를 정제해야 할 때도 있음.
            예를 들어 데이터 값이 null이거나 아예 비어 있거나, 또는 결측 값에 준하는 문자열 상수(N/A 또는 unknown)를 데이터에 포함하기도 함.
            이때는 DataFrame의 na 필드로 제공되는 DataFrameNaFunctions를 활용해 결측 값이 있는 데이터 인스턴스를 적절하게 처리할 수 있음.
            null 또는 NaN 등 결측 값을 가진 로우를 DataFrame에서 제외하거나 결측 값 대신 다른 상수를 채워 넣거나
            결측 값에 준하는 특정 칼럼 값을 다른 상수로 치환하는 방법을 사용함
         */

        // drop : null이나 NaN 값을 가진 모든 로우를 DataFrame에서 제외할 수 있음.
        // drop("any") : 어떤 칼럼에서든 null 값을 발견하면 해당 로우를 제외함.
        // drop("all") : 모든 컬럼 값이 null인 로우만 제외
        // 컬럼 지정도 가능
        postDF.count        // Long = 1261
        val cleanPost = postDF.na.drop()
        cleanPost.count     // Long = 222

        // 답변 ID가 없는 로우 제외
        postDF.na.drop(Array("acceptedAnswerId")).count

        // fill 함수를 사용해 null 또는 NaN 값을 채울 수 있음.
        // fill 함수의 인수를 하나만 지정하면 이 인수 값을 모든 컬럼의 결측 값을 대체할 상수로 사용함.
        // 특정 컬럼을 지정하려면 두 번째 인수에 칼럼 이름의 목록을 전달해야 함.
        // 또는 컬럼 이름과 대체 값을 매핑한 Map 객체를 전달할 수도 있음.
        postDF.na.fill(Map("viewCount" -> 0)).select($"viewCount").show

        // replace 함수를 사용해 특정 컬럼의 특정 값을 다른 값으로 치환할 수 있음.
        postDF.filter($"id" === 1177 || $"acceptedAnswerId" === 1177).count     // Long = 2
        postDF.na.replace(Array("id", "acceptedAnswerId"), Map(1177 -> 3000)).
            filter($"id" === 1177 || $"acceptedAnswerId" === 1177).count        // Long = 0

        /*
        DataFrame을 RDD로 변환
            DataFrame을 RDD로 변환하면 RDD는 org.apache.spark.sql.Row 타입의 요소로 구성됨.
            Row 클래스는 칼럼 번호로 해당 칼럼 값을 가져올 수 있는 다양한 get 함수를 제공함. getString(index), getInt(index), getMap(index) 등
            또 mkString(delimiter) 함수를 사용해 각 로우를 손쉽게 문자열로 만들 수 있음.

            DataFrame 데이터와 파티션을 map이나 flatMap, mapPartitions 변환 연산자 등으로 매핑하면 실제 매핑 작업은 하부 RDD에서 실행됨.
            그러므로 변환 연산의 결과 또한 새로운 DataFrame이 아니라 새로운 RDD가 됨.
            RDD의 변환 연산자의 모든 내용이 DataFrame 변환 연산에도 동일하게 적용되며,
            변환 연산자에 Row 객체를 처리하는 함수를 전달해야 한다는 제약 사항이 추가됨.

            또 DataFrame의 변환 연산자는 DataFrame 스키마(즉, RDD 스키마)를 변경할 수 있음.
            다시 말해 칼럼 순서나 개수, 타입을 변경할 수 있음. 또는 RDD의 Row 객체를 다른 타입으로 변환할 수도 있음.
            하지만 타입을 변경한 RDD를 다시 DataFrame으로 변환하는 과정은 자동화할 수 없음.
            변환 연산자가 DataFrame 스키마를 변경하지 않았다면 DataFrame의 이전 스키마를 그대로 사용해 새 DataFrame을 생성할 수 있음.

            예로 body 칼럼과 tags 칼럼에 포함된 &lt;와 &gt; 문자열을 각각 <와 > 문자로 바꾸어 보자.
            먼저 각 로우를 Seq 객체로 매핑한 후 Seq 객체의 updated 메서드를 사용해 Seq의 요소값을 변경함.
            그런 다음 updated 결과로 반환된 Seq를 다시 Row 객체로 매핑하고, 원본 DataFrame의 스키마를 사용해 새 DataFrame을 만들 수 있음.

            하지만 DataFrame API의 내장 DSL, SQL 함수, 사용자 정의 함수 등으로 거의 모든 매핑 작업을 해결할 수 있기 때문에
            DataFrame을 RDD로 변환하고 다시 DataFrame으로 만들어야 할 경우는 매우 드물다고 할 수 있음.
         */
        val postsRdd = postDF.rdd   // org.apache.spark.rdd.RDD[org.apache.spark.sql.Row]

        val postsMapped = postsRdd.map(row => {
            Row.fromSeq(row.toSeq.updated(3, row.getString(3).replace("&lt;", "<").replace("&gt;", ">")).
                updated(8, row.getString(8).replace("&lt;", "<").replace("&gt;", ">")))
        })
        val postDFNew = spark.createDataFrame(postsMapped, postSchema)
        postDFNew.show(3)

        /*
        Data Grouping
            groupBy 함수는 컬럼 이름 또는 Column 객체의 목록을 받고 GroupedData 객체를 반환함.

            GroupedData는 groupBy에 지정한 컬럼들의 값이 모두 동일한 로우 그룹들을 표현한 객체로,
            각 그룹을 대상으로 값을 집계할 수 있는 표준 집계 함수를 제공함.
            GroupedData의 각 집계 함수는 groupBy에 지정한 칼럼들과 집계 결과를 저장한 추가 컬럼으로 구성된 DataFrame을 반환함.
         */
        postDFNew.groupBy($"ownerUserId", $"tags", $"postTypeId").count.orderBy($"ownerUserId" desc).show(10)

        // agg 함수를 사용해 서로 다른 컬럼의 여러 집계 연산을 한꺼번에 수행할 수도 있음.
        // 각 컬럼 이름과 집계 함수 이름을 매핑한 Map 객체를 전달할 수 있음.
        postDFNew.groupBy($"ownerUserId").
            agg(max($"lastActivityDate"), max($"score")).orderBy($"max(score)" desc).show(10)

        postDFNew.groupBy($"ownerUserId").
            agg(Map("lastActivityDate" -> "max", "score" -> "max")).orderBy(desc("max(score)")).show(10)

        // 첫 번째 방식을 사용하면 여러 컬럼의 표현식을 연결할 수 있음
        postDFNew.groupBy($"ownerUserId").
            agg(max($"lastActivityDate"), max($"score").gt(5)).show(10)

        /*
        rollup & cube
            추가적인 데이터 그룹핑 및 집계 기능을 제공함.
            groupBy는 지정된 컬럼들이 가질 수 있는 값의 모든 조합별로 집계 연산을 수행함.
            반면 rollup과 cube는 지정된 컬럼의 부분 집합을 추가로 사용해 집계 연산을 수행함.
            cube는 컬럼의 모든 조합을 대상으로 계산하는 반면,
            rollup은 지정된 컬럼 순서를 고려한 순열을 사용함.
         */
        val smplDF = postDFNew.where($"ownerUserId" >= 13 and $"ownerUserId" <= 15)
        smplDF.groupBy('ownerUserId, 'tags, 'postTypeId).count.show()
        smplDF.rollup($"ownerUserId", $"tags", $"postTypeId").count.show()
        /*
        +-----------+----+----------+-----+
        |ownerUserId|tags|postTypeId|count|
        +-----------+----+----------+-----+
        |       null|null|      null|    5|
        |         13|    |         2|    1|
        |         13|null|      null|    1|
        |         13|    |      null|    1|
        |         15|    |         2|    2|
        |         15|    |      null|    2|
        |         14|    |      null|    2|
        |         15|null|      null|    2|
        |         14|null|      null|    2|
        |         14|    |         2|    2|
        +-----------+----+----------+-----+
         */
        smplDF.cube($"ownerUserId", $"tags", $"postTypeId").count.show()
        /*
        +-----------+----+----------+-----+
        |ownerUserId|tags|postTypeId|count|
        +-----------+----+----------+-----+
        |       null|    |         2|    5|
        |       null|null|         2|    5|
        |       null|null|      null|    5|
        |         13|    |         2|    1|
        |         13|null|         2|    1|
        |       null|    |      null|    5|
        |         13|null|      null|    1|
        |         13|    |      null|    1|
        |         15|    |         2|    2|
        |         15|    |      null|    2|
        |         14|    |      null|    2|
        |         15|null|      null|    2|
        |         14|null|      null|    2|
        |         15|null|         2|    2|
        |         14|    |         2|    2|
        |         14|null|         2|    2|
        +-----------+----+----------+-----+
         */

        // DataFrame Join
        val itVotesRaw = sc.textFile("italianVotes.csv").map(x => x.split("~"))
        val itVotesRows = itVotesRaw.map(row => Row(row(0).toLong, row(1).toLong, row(2).toInt, Timestamp.valueOf(row(3))))
        val voteSchema = StructType(
            Seq(
                StructField("id", LongType, nullable = false),
                StructField("postId", LongType, nullable = false),
                StructField("voteTypeId", IntegerType, nullable = false),
                StructField("creationDate", TimestampType, nullable = false)
            )
        )
        val voteDF = spark.createDataFrame(itVotesRows, voteSchema)
        val postVote = postDF.join(voteDF, postDF.col("id") === 'postId)
        postVote.show(5)

        val postVoteOuter = postDF.join(voteDF, postDF("id") === $"postId")
        postVoteOuter.show(5)

        // 스파크 SQL
        // DataFrame을 테이블로 등록 -> 테이블 카탈로그에 저장
        // 하이브 지원하는 SparkSession에서는 테이블 카탈로그가 하이브 메타스토어에 저장됨(영구. 스파크 세션을 종료하고 새로 시작해도 DataFrame 정보를 유지함)

        // 테이블을 임시로 등록
        postDF.createOrReplaceTempView("post_temp")

        // 테이블을 영구적으로 등록하려면 하이브를 지원하는 SparkSession을 사용해야 함.
        // HiveContext는 metastore_db 폴더 아래 로컬 작업 디렉터리에 Derby 데이터베이스를 생성함
        // 작업 디렉터리 위치를 변경하려면 hive-site.xml 파일의 hive.metastore.warehouse.dir에 원하는 경로를 지정
        postDF.write.saveAsTable("posts")

        // 기존 테이블을 덮어쓰려면
        postDF.write.mode("overwrite").saveAsTable("posts")
        /*
        Writer 설정
            - format
                데이터를 저장할 파일 포맷, 즉 데이터 소스 이름을 지정함.
                기본 데이터 소스(json, parquet, orc)나 커스텀 데이터 소스 이름을 사용할 수 있음.
                format을 지정하지 않으면 기본으로 parquet을 사용

            - mode
                지정된 테이블 또는 파일이 이미 존재하면 이에 대응해 데이터를 저장할 방식을 지정.
                overwrite(기존 데이터를 덮어씀), append(기존 데이터에 추가함),
                ignore(아무것도 하지 않고 저장 요청 무시), error(예외) 중 하나 지정함.

            - option과 options
                데이터 소스를 설정할 매개변수 이름과 변수 값을 추가(options 메서드에는 매개변수의 이름-값 쌍을 map 형태로 전달)

            - partitionBy
                복수의 파티션 컬럼을 지정
         */

        // JDBC 메서드로 관계형 데이터베이스에 데이터 저장
        // 모든 파티션이 관계형 데이터베이스와 연결해 각자 데이터를 저장하기 때문에,
        // DataFrame의 파티션이 너무 많으면 데이터베이스에 부담을 줄 수 있음.
        val props = new Properties()
        props.setProperty("user", "username")
        props.setProperty("password", "pw")
        postDF.write.jdbc("jdbc:postgresql://postgresrv/mydb", "posts", props)

        // 스파크 테이블 카탈로그
        // 현재 등록된 테이블 목록을 조회할 수 있음
        // 테이블 정보는 메타스토어 데이터베이스에 등록됨. 기본 데이터베이스 이름은 default이며,
        // 이 데이터베이스의 MANAGED 테이블은 사용자 홈 디렉터리 아래 spark_warehouse 폴더에 저장됨.
        // 저장 위치를 변경하려면 spark.sql.warehouse.dir 매개변수에 원하는 위치를 설정함.

        spark.catalog.listTables().show()
        /*
        +---------+--------+-----------+---------+-----------+
        |     name|database|description|tableType|isTemporary|
        +---------+--------+-----------+---------+-----------+
        |    posts| default|       null|  MANAGED|      false|
        |post_temp|    null|       null|TEMPORARY|       true|
        +---------+--------+-----------+---------+-----------+
         */

        // Catalog 객체를 사용해 특정 테이블의 컬럼 정보를 조회할 수 있음
        spark.catalog.listColumns("posts").show()

        // SQL 함수 목록도 조회할 수 있음
        spark.catalog.listFunctions.show

        // cache, uncacheTable, isCached, clearCache 등 메서드를 사용해 테이블을 메모리에 캐시하거나 삭제할 수 있음.

        /*
        원격 하이브 메타스토어 설정
            스파크가 원격지의 하이브 메타스토어 데이터베이스를 사용하도록 설정하는 것도 가능.
            스파크의 원격지 메타스토어로는 기존에 설치한 하이브 메타스토어 데이터베이스를 활용하거나,
            스파크가 단독으로 사용할 새로운 데이터베이스를 구축할 수도 있음.
            하이브 메타스토어는 스파크 conf 디렉터리 아래에 있는 하이브 설정 파일의 매개변수들로 설정하며,
            이 파일을 설정하면 spark.sql.warehouse.dir 매개변수는 오버라이드 됨.

            hive-site.xml 파일에는 반드시 configuration 태그를 하나만 입력해야 하며, configuration 태그 안에 여러 property 태그를 넣을 수 있음.
            각 property 태그는 name과 value 태그로 구성됨.

            hive-site.xml 예
            <?xml version="1.0"?>
            <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
            <configuration>
                <property>
                        <name>hive.metastore.warehouse.dir</name>
                        <value>/hive/metastore/directory</value>
                </property>
            </configuration>

            스파크가 원격 하이브 메타스토어를 사용하도록 설정하려면 다음 property들을 hive-site.xml 파일에 추가해야 함.
                - javax.jdo.option.ConnectionURL: JDBC 접속 URL
                - javax.jdo.option.ConnectionDriverName: JDBC 드라이버의 클래스 이름
                - javax.jdo.option.ConnectionUserName: 데이터베이스 사용자 이름
                - javax.jdo.option.ConnectionPassword: 데이터베이스 사용자 암호

            property에 지정한 JDBC 드라이버를 스파크 드라이버와 모든 익스큐터의 클래스패스에 추가해야 함.
            가장 간단한 방법은 사용할 JDBC 드라이버의 JAR 파일을 spark-submit이나 spark-shell 명령의 --jars 옵션으로 전달하는 것임.
         */
    }
}
