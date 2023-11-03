// RDD 계보
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object LineageEx {
    def main(args: Array[String]) = {
        val sc = new SparkContext(new SparkConf().setMaster("local[*]"))

        val rdd1 = sc.parallelize(Seq(1, 2, 3, 4, 5, 6))                // org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0] at parallelize at <console>:23
        val rdd2 = rdd1.map(i => i * 3)                                 // org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[1] at map at <console>:23
        val rdd3 = rdd2.map(i => i + 2)                                 // org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[2] at map at <console>:23

        rdd1.toDebugString          // String = (6) ParallelCollectionRDD[0] at parallelize at <console>:23 []
        rdd2.toDebugString          // (6) MapPartitionsRDD[1] at map at <console>:23 []
                                    // |  ParallelCollectionRDD[0] at parallelize at <console>:23 []
        rdd3.toDebugString          // (6) MapPartitionsRDD[2] at map at <console>:23 []
                                    // |  MapPartitionsRDD[1] at map at <console>:23 []
                                    // |  ParallelCollectionRDD[0] at parallelize at <console>:23 []

        // RDD는 첫 번째 RDD와 동일한 데이터 타입일 필요는 없음.
        // 다양한 데이터 타입을 저장하는 RDD 예시
        val rdd4 = rdd3.map(i => ("str" + (i + 2).toString, i - 2))     // org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[3] at map at <console>:23
        rdd4.take(10)               // Array[(String, Int)] = Array((str7,3), (str10,6), (str13,9), (str16,12), (str19,15), (str22,18))
    }
}