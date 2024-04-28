import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TransFormEx {
    def main(args: Array[String]) = {
        val sc = new SparkContext(new SparkConf().setMaster("local[*]"))
        
        // map
        // 입력 파티션에 트랜스포메이션 함수를 적용해 출력 RDD에 출력 파티션을 생성
        val rdd1 = sc.textFile("wiki1.txt")     // org.apache.spark.rdd.RDD[String] = wiki1.txt MapPartitionsRDD[1] at textFile at <console>:23
        rdd1.count              // Long = 9
        rdd1.first              // String = ...

        val rdd2 = rdd1.map(line => line.length)    // org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[2] at map at <console>:23
        rdd2.take(10)           // Array[Int] = Array(271, 165, 146, 138, 231, 159, 159, 410, 281)

        // parallelize 메서드는 Seq 객체(Array, List 등)를 받아 새로운 RDD 만듦.
        val numbers = sc.parallelize(10 to 50 by 10)
        numbers.foreach(x => println(x))

        val numbersSquared = numbers.map(num => num * num)
        numbersSquared.foreach(println)

        val reversed = numbersSquared.map(x => x.toString.reverse)
        reversed.foreach(println)

        // _ : placeholder
        val alsoReversed = numbersSquared.map(_.toString.reverse)
        alsoReversed.first
        // RDD 컬렉션에서 값이 가장 큰 요소 K개가 내림차순으로 정렬된 배열을 말함
        alsoReversed.top(4)

        val lines = sc.textFile("client-ids.txt")
        lines.txt           // String = 15,16,20,20
        
        val idsStr = lines.map(x => x.split(","))
        idsStr.foreach(println(_))
        /*
        [Ljava.lang.String;@2a414f18
        [Ljava.lang.String;@6f17afc
        [Ljava.lang.String;@ecbeb08
        [Ljava.lang.String;@61a2d059
            각 줄을 쉼표로 분할한 결과, 배열로 idsStr RDD를 구성함.
            따라서 이 결과는 Array.toString 메서드가 반환한 값을 출력한 것.
        */
        idsStr.first                    // Array[String] = Array(15, 16, 20, 20)
        // collect (RDD Action 연산자)
        idsStr.collect                  // Array[Array[String]] = Array(Array(15, 16, 20, 20), Array(77, 80, 94), Array(94, 98, 16, 31), Array(31, 15, 20))


        // flatMap
        // 배열의 배열을 단일 배열로 분해
        // 익명 함수가 반환한 배열의 중첩 구조를 한 단계 제거하고 모든 배열의 요소를 단일 컬렉션으로 병합
        // 입력 RDD 엘리먼트의 모든 컬렉션을 flat하게 함
        val ids = lines.flatMap(_.split(","))
        ids.collect                     // Array[String] = Array(15, 16, 20, 20, 77, 80, 94, 94, 98, 16, 31, 31, 15, 20)
        // mkString은 배열의 모든 요소를 문자열 하나로 병합함(스칼라 표준 라이브러리)
        ids.collect.mkString("; ")      // String = 15; 16; 20; 20; 77; 80; 94; 94; 98; 16; 31; 31; 15; 20
        ids.first                       // 15

        // ids RDD는 String 타입인데 알파벳 순으로 정렬되는 상황을 방지하기 위해 Int 타입으로 변환할 필요 있음.
        val intIds = ids.map(_.toInt)
        intIds.collect
        // distinct : 해당 RDD의 고유 요소로 새로운 RDD를 생성함
        // def distinct(): RDD[T]
        val uniqueIds = intIds.distinct
        uniqueIds.collect
        uniqueIds.count
        ids.count

        val rdd3 = rdd1.map(line => line.split(" "))    // org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[3] at map at <console>:23
        rdd3.take(1)            // Array[Array[String]] = Array(Array(Apache, Spark, provides, programmers, ...

        val rdd4 = rdd1.flatMap(line => line.split(" "))    // org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[4] at flatMap at <console>:23
        rdd4.take(10)           // Array[String] = Array(Apache, Spark, provides, programmers, with, an, application, programming, interface, centered)


        // filter
        val rdd5 = rdd1.filter(line => line.contains("Spark"))  // org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[5] at filter at <console>:23
        rdd5.count              // Long = 5


        // sample
        val s = uniqueIds.sample(false, 0.3)                    // org.apache.spark.rdd.RDD[Int] = PartitionwiseSampledRDD[19] at sample at <console>:23
        s.count                 // Long = 4
        s.collect               // Array[Int] = Array(16, 98, 94, 31)

        val swr = uniqueIds.sample(true, 0.5)
        swr.count
        swr.collect
        
        // takeSample(Action)
        val taken = uniqueIds.takeSample(false, 5)
        uniqueIds.take(5)


        // coalesce
        rdd1.partitions.length  // Int = 2
        
        val rdd6 = rdd1.coalesce(1)     // org.apache.spark.rdd.RDD[String] = CoalescedRDD[6] at coalesce at <console>:23
        rdd6.partitions.length  // Int = 1


        // repartition
        // 입력 RDD를 출력 RDD에서 더 적거나 더 많은 출력 파티션으로 다시 파티셔닝하기 위해
        val rdd7 = rdd1.repartition(5)  // org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[10] at repartition at <console>:23
        rdd7.partitions.length  // Int = 5 
    }
}