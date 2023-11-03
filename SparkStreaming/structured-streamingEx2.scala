import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder().getOrCreate()

import java.sql.Timestamp

// '호스트:포트'에 대한 커넥션에서 입력 라인 스트림을 의미하는 데이터 프레임 생성
val inputLines = spark.readStream.
    format("socket").
    option("host", "localhost").
    option("port", 9999).
    option("includeTimestamp", true).
    load()

// 타임스탬프를 포함한 라인을 단어로 나눔
import spark.implicits._
val words = inputLines.as[(String, Timestamp)].
    flatMap(line => line._1.split(" ").map(word => (word, line._2))).toDF("word", "timestamp")

// 윈도우와 단어별로 그룹핑하고 각 그룹별로 단어 개수를 계산함.
val windowedCounts = words.withWatermark("timestamp", "10 seconds").
    groupBy(window('timestamp, "10 seconds", "10 seconds"), 'word).
    count().orderBy('window)

// 윈도우 안의 단어 개수를 콘솔에 출력하는 쿼리를 실행함.
val query = windowedCounts.writeStream.
    outputMode("complete").
    format("console").
    option("truncate", "false").
    start()

query.awaitTermination()

/*
-------------------------------------------
Batch: 2
-------------------------------------------
+------------------------------------------+-----+-----+
|window                                    |word |count|
+------------------------------------------+-----+-----+
|{2022-09-20 21:01:30, 2022-09-20 21:01:40}|hi   |3    |
|{2022-09-20 21:01:40, 2022-09-20 21:01:50}|hi   |6    |
|{2022-09-20 21:01:40, 2022-09-20 21:01:50}|hello|1    |
+------------------------------------------+-----+-----+
*/