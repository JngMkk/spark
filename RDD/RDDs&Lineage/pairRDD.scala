import org.apache.spark.sql.SparkSession

object pairRDDEx {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().getOrCreate()
        val sc = spark.sparkContext

        val homeDir = System.getenv("HOME")
        val tranFile = sc.textFile(homeDir + "ch04_data_transactions.txt")
        tranFile.collect                            // Array(2015-03-30#6:55 AM#51#68#1#9506.21, 2015-03-30#7:39 PM#99#86#5#4107.59, 2015-03-30#11:57 AM#79#58#7#2987.22, ...
        // 구매 날짜, 시간, 고객 ID, 상품 ID, 구매 수량, 구매 금액이 순서대로 기록되어 있음.

        val tranData = tranFile.map(_.split("#"))
        tranData.take(5)                            // Array(Array(2015-03-30, 6:55 AM, 51, 68, 1, 9506.21), Array(2015-03-30, 7:39 PM, 99, 86, 5, 4107.59), Array(2015-03-30, 11:57 AM, 79, 58, 7, 2987.22), ...

        // 고객 ID를 키로
        var transByCust = tranData.map(x => (x(2).toInt, x))
        transByCust.take(5)                         // Array[(Int, Array[String])] = Array((51,Array(2015-03-30, 6:55 AM, 51, 68, 1, 9506.21)), (99,Array(2015-03-30, 7:39 PM, 99, 86, 5, 4107.59)), ...

        // 고객 ID 고유 개수
        transByCust.keys.distinct().count()         // Long = 100

        // 각 키의 출현 횟수를 스칼라 Map 형태로 반환
        transByCust.countByKey()                    // scala.collection.Map[Int,Long] = Map(69 -> 7, 88 -> 5, 5 -> 11, ...

        // Map에 담긴 값을 모두 더하면 당연히 총 구매 횟수가 됨.
        transByCust.countByKey().values.sum         // Long = 1000

        transByCust.countByKey().toSeq              // Seq[(Int, Long)] = Vector((69,7), (88,5), (5,11), (10,7), (56,17),

        // 구매 횟수가 가장 많았던 고객
        transByCust.countByKey().toSeq.sortBy(_._2).last    // (Int, Long) = (53,19)
        transByCust.countByKey().toSeq.maxBy(_._2)

        var complTrans = Array(Array("2015-03-03", "11:59 PM", "53", "4", "1", "0.0"))

        // 특정 키의 모든 값 가져오기
        // lookup 연산자는 결과 값을 드라이버로 전송하므로 이를 메모리에 적재할 수 있는지 먼저 확인해야 함.
        transByCust.lookup(53)                      // Seq[Array[String]] = WrappedArray(Array(2015-03-30, 6:18 AM, 53, 42, 5, 2197.85), ...

        // WrappedArray 클래스는 암시적 변환을 이용해 Array를 Seq(가변 컬렉션)로 표현함.
        // mkString은 배열의 모든 요소를 문자열 하나로 병합함(스칼라 표준 라이브러리)
        transByCust.lookup(53).foreach(x => println(x.mkString(", ")))

        // mapValues 변환 연산자로 Pair RDD 값 바꾸기
        // 키를 변경하지 않고 Pair RDD에 포함된 값만 변경할 수 있음
        transByCust = transByCust.mapValues(x => {
            if(x(3).toInt == 25 && x(4).toDouble > 1)
                x(5) = (x(5).toDouble * 0.95).toString
            x
        })

        /*
            flatMapValues 변환 연산자로 키에 값 추가
                각 키 값을 0개 또는 한 개 이상 값으로 매핑해 RDD에 포함된 요소 개수를 변경할 수 있음.
                키에 새로운 값을 추가하거나 키 값을 모두 제거할 수 있음.
                flatMapValues를 호출하려면 V => TraversableOnce[U] 형식의 변환 함수를 전달해야 함.
                flatMapValues는 변환 함수가 반환한 컬렉션 값들을 원래 키와 합쳐 새로운 키-값 쌍으로 생성함.
        */
        transByCust = transByCust.flatMapValues(x => {
            if(x(3).toInt == 81 && x(4).toDouble >= 5) {
                val cloned = x.clone()      // 구매 기록 배열 복제
                cloned(5) = "0.00"; cloned(3) = "70"; cloned(4) = "1";
                List(x, cloned)             // 원래 요소와 추가한 요소 반환
            } else {
                List(x)
            }
        })

        transByCust.count       // Long = 1006

        /*
            reduceByKey 변환 연산자로 키의 모든 값 병합
                각 키의 모든 값을 동일한 타입의 단일 값으로 병합함
                두 값을 하나로 병합하는 merge 함수를 전달해야 함. 각 키별로 값 하나만 남을 때까지 merge 함수를 계속 호출함.
                따라서 merge 함수는 결합 법칙을 만족해야 함. 만족하지 않으면 같은 RDD라도 reduceByKey를 호출할 때마다 결과 값이 달라짐.

                foldBykey로 대체할 수도 있음.
                foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)]
                reduceByKey와 기능은 같지만, merge 함수 인자 목록 바로 앞에 zeroValue 인자를 담은 또 다른 인자 목록을 추가로 전달해야 한다는 점이 다름.

                zeroValue는 반드시 항등원이어야 함(예를 들어 덧셈 연산에서는 0, 곱셈에서는 1, 리스트 연산에서는 Nil 등이 항등원임)
                zeroValue는 가장 먼저 func 함수로 전달해 키의 첫 번째 값과 병합하며, 이 결과를 다시 키의 두 번째 값과 병합함.
        */
        val amounts = transByCust.mapValues(x => x(5).toDouble)
        amounts.take(5)                     // Array[(Int, Double)] = Array((51,9506.21), (99,4107.59), (79,2987.22), (51,7501.89), (86,8370.2))

        val totals = amounts.foldByKey(0)((x, y) => x + y).collect()
        totals.toSeq.sortBy(_._2).last      // (Int, Double) = (76,100049.0)
        totals.toSeq.maxBy(_._2)

        complTrans = complTrans :+ Array("2015-03-30", "11:59 PM", "76", "63", "1", "0.00")
        transByCust = transByCust.union(sc.parallelize(complTrans).map(x => (x(2).toInt, x)))
        transByCust.map(x => x._2.mkString("#")).saveAsTextFile("ch4output-transByCust")

        // aggregateByKey로 키의 모든 값 그룹핑
        // zerovalue와 함께 [(U, V) => U] 변환 함수, [(U, U) => U] 병합 함수를 전달해야 함.
        val prods = transByCust.aggregateByKey(List[String]())((prods, x) => prods ::: List(x(3)), (prods1, prods2) => prods1 ::: prods2)
        prods.collect()
    }
}
