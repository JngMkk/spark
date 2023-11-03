import org.apache.spark.storage.StorageLevel


object CachingEx {
    def main(args: Array[String]) = {
        val rdd1 = sc.parallelize(Seq(1, 2, 3, 4, 5, 6))    // org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0] at parallelize at <console>:24
        rdd1.persist(StorageLevel.MEMORY_ONLY)              // rdd1.type = ParallelCollectionRDD[0] at parallelize at <console>:24
        rdd1.unpersist()                                    // rdd1.type = ParallelCollectionRDD[0] at parallelize at <console>:24
        rdd1.persist(StorageLevel.DISK_ONLY)                // rdd1.type = ParallelCollectionRDD[0] at parallelize at <console>:24
        rdd1.unpersist()                                    // rdd1.type = ParallelCollectionRDD[0] at parallelize at <console>:24

        // 웹 UI에서 캐싱 전 후 Duration 확인 가능
        // Storage 탭에서 캐싱 확인 가능
        val rdd2 = sc.parallelize(Seq(1, 2, 3, 4, 5, 6))    // org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[1] at parallelize at <console>:23
        rdd2.count      // Long = 6
        rdd2.cache      // rdd2.type = ParallelCollectionRDD[1] at parallelize at <console>:23
        rdd2.count      // Long = 6
    }
}