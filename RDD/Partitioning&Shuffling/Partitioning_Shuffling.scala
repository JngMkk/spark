import org.apache.spark._

object PartitionAndShufflingEx {
    def main(args: Array[String]) = {
        val sc = new SparkContext(new SparkConf().setMaster("local[*]"))

        // pairRDD의 RangePartitioning을 사용하는 방법에 대한 예
        val statesPopRdd = sc.textFile("statesPopulation.csv")
        val pairRdd = statesPopRdd.map(x => (x.split(",")(0), 1))
        val rangePartitioner = new RangePartitioner(5, pairRdd)             // org.apache.spark.RangePartitioner[String,Int] = org.apache.spark.RangePartitioner@ddeed05f
        rangePartitioner.numPartitions                                      // Int = 5
        val rangePartitionedRdd = pairRdd.partitionBy(rangePartitioner)     // org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[5] at partitionBy at <console>:25
        rangePartitionedRdd.take(10)                                        // Array[(String, Int)] = Array((Alabama,1), (Alaska,1), (Arizona,1), (Arkansas,1), (California,1), (Colorado,1), (Delaware,1), (District of Columbia,1), (Florida,1), (Georgia,1))

        // 파티션 번호, 개수
        // (Int, Iterator[T]) => Iterator[U]
        pairRdd.mapPartitionsWithIndex((i, x) => Iterator("" + i + ":" + x.length)).take(10)                // Array[String] = Array(0:177, 1:174)
        rangePartitionedRdd.mapPartitionsWithIndex((i, x) => Iterator("" + i + ":" + x.length)).take(10)    // Array[String] = Array(0:77, 1:63, 2:77, 3:70, 4:64)
    }
}