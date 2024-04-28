import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ActionEx {
    def main(args: Array[String]) = {
        val sc = new SparkContext(new SparkConf().setMaster("local[*]"))

        // reduce
        // RDD의 모든 엘리먼트에 reduce 함수를 적용하고 적용한 결과를 드라이버에 전달.
        val rdd1 = sc.parallelize(Seq(1, 2, 3, 4, 5, 6))    // org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[11] at parallelize at <console>:23
        rdd1.take(10)                       // Array[Int] = Array(1, 2, 3, 4, 5, 6)
        rdd1.reduce((a, b) => a + b)        // Int = 21


        // count
        // RDD의 엘리먼트 개수를 단순히 계산해 드라이버에 보냄
        rdd1.count                          // Long = 6

        
        // collect
        // RDD의 모든 엘리먼트를 얻은 결과를 드라이버에 보냄
        val rdd2 = sc.textFile("wiki1.txt") // org.apache.spark.rdd.RDD[String] = wiki1.txt MapPartitionsRDD[13] at textFile at <console>:23
        rdd2.collect                        // Array[String] = Array(Apache Spark provides programmers ...
    }
}