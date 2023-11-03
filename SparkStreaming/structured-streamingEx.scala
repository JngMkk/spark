// 로컬 호스트의 9999 포트에서 단어를 읽는 스트림을 생성함.
// nc -lk 9999
import org.apache.spark.sql.SparkSession
import spark.implicits._
val spark = SparkSession.builder().getOrCreate()

val inputLines = spark.readStream.
    format("socket").
    option("host", "localhost").
    option("port", 9999).
    load()

// inputLines을 단어로 나눔
// as : def as[U: Encoder]: Dataset[U]
// Dataset 내용을 해석할 방법을 정의한 Encoder 객체를 전달해야 함
val words = inputLines.as[String].flatMap(_.split(" "))

// 단어 개수를 얻음
val wordCounts = words.groupBy("value").count()

val query = wordCounts.writeStream.outputMode("complete").format("console")

query.start()

/*
-------------------------------------------
Batch: 1
-------------------------------------------
+-----+-----+
|value|count|
+-----+-----+
|   my|    1|
|   hi|    1|
+-----+-----+
*/