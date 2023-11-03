import org.apache.spark._

object BroadcastVariableEx {
    def main(args: Array[String]) = {
        val sc = new SparkContext(new SparkConf().setMaster("local[*]"))


        // 브로드캐스트 변수 생성
        // 데이터나 변수가 직렬화 가능한 경우 모든 데이터 타입의 데이터에 스파크 컨텍스트의 broadcast 함수를 사용해 브로드캐스트 변수를 생성할 수 있음.
        val rdd1 = sc.parallelize(Seq(1, 2, 3))
        val i = 5
        val bi = sc.broadcast(i)                        // org.apache.spark.broadcast.Broadcast[Int] = Broadcast(8)
        bi.value                                        // Int = 5
        rdd1.take(5)                                    // Array[Int] = Array(1, 2, 3)
        rdd1.map(j => j + bi.value).take(5)             // Array[Int] = Array(6, 7, 8)

        val m = scala.collection.mutable.HashMap(1 -> 2, 2 -> 3, 3 -> 4)    // scala.collection.mutable.HashMap[Int,Int] = Map(2 -> 3, 1 -> 2, 3 -> 4)
        val bm = sc.broadcast(m)                                            // org.apache.spark.broadcast.Broadcast[scala.collection.mutable.HashMap[Int,Int]] = Broadcast(15)
        bm.value                                                            // scala.collection.mutable.HashMap[Int,Int] = Map(2 -> 3, 1 -> 2, 3 -> 4)
        rdd1.map(j => j * bm.value(j)).take(5)                              // Array[Int] = Array(2, 6, 12)


        // 브로드캐스트 변수 정리
        // 모든 익스큐터에서 메모리를 차지하기 때문에 변수에 포함된 데이터의 크기에 따라 특정 시점에서 자원 문제가 발생할 수 있음.
        // unpersist를 호출한 후 브로드캐스트 변수에 다시 접근하면 일반적으로 동작하지만 배후에서 익스큐터는 변수에 대한 데이터를 다시 가져옴
        bi.unpersist
        rdd1.map(j => j + bi.value).take(5)             // Array[Int] = Array(6, 7, 8)


        // 브로드캐스트 정리
        // 모든 익스큐터와 드라이버에서 브로드캐스트 변수를 완전히 정리해 접근할 수 없게 브로드캐스트를 정리할 수도 있음.
        // 클러스터에서 자원을 최적화해서 관리할 때 매우 유용
        bi.destroy
        rdd1.map(j => j + bi.value).take(5)             // 22/09/16 11:49:18 ERROR Utils: Exception encountered
                                                        // org.apache.spark.SparkException: Attempted to use Broadcast(8) after it was destroyed (destroy at <console>:25)
    }
}