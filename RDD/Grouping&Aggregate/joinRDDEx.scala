import org.apache.spark.sql.SparkSession
import org.apache.spark.HashPartitioner
/*
    - 어제 판매한 상품 이름과 각 상품별 매출액 합계(알파벳 오름차순으로 정렬할 것)
    - 어제 판매하지 않은 상품 목록
    - 전일 판매 실적 통계 : 각 고객이 구입한 상품의 평균 가격, 최저 가격 및 최고 가격, 구매 금액 합계
*/

object joinRDDEx {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().getOrCreate()
        val sc = spark.sparkContext

        // Join
        // 어제 판매한 상품 이름과 각 상품별 매출액 합계
        val tranFile = sc.textFile("ch04_data_transactions.txt")
        val tranData = tranFile.map(_.split("#"))
        // 구매 날짜, 시간, 고객 ID, 상품 ID, 구매 수량, 구매 금액이 순서대로 기록되어 있음.
        val transByCust = tranData.map(x => (x(2).toInt, x))
        val transByProd = tranData.map(x => (x(3).toInt, x))

        // 각 상품의 매출액 합계 계산
        val totalsByProd = transByProd.mapValues(x => x(5).toDouble).reduceByKey((tot1, tot2) => tot1 + tot2)
        // val totalsByProd2 = transByProd.mapValues(x => x(5).toDouble).reduceByKey{case(tot1, tot2) => tot1 + tot2}

        // 어제 판매한 상품 이름을 알아내려면 상품 이름 정보 파일과 전일 구매 기록을 조인해야 함.
        val products = sc.textFile("ch04_data_products.txt").map(x => x.split("#")).map(p => (p(0).toInt, p))

        // 조인 연산자를 호출하려면 (K, V) 타입의 요소를 가진 RDD에 (K, W) 타입의 요소를 가진 다른 RDD를 전달해야 함.
        // 다른 Pair RDD 변환 연산자와 마찬가지로 조인 연산자에 Partitioner 객체나 파티션 개수를 전달할 수 있음.
        // Partitioner를 지정하지 않으면 조인할 두 RDD 중 첫 번째 RDD의 Partitioner를 사용함.
        // 두 RDD에 Partitioner를 명시적으로 정의하지 않으면 스파크는 새로운 HashPartitioner를 생성함.
        // 이때 파티션 개수로 spark.default.partitions 매개변수 값을 사용하거나 두 RDD의 파티션 개수 중 더 큰 수를 사용함.
        val totalsAndProds = totalsByProd.join(products)
        totalsAndProds.first()          // (Int, (Double, Array[String])) = (34,(62592.43000000001,Array(34, GAM X360 Assassins Creed 3, 6363.95, 9)))

        // 어제 판매하지 않은 상품 목록은 여집합 연산을 통해 구할 수 있음.
        val totalsWithMissingProds = products.leftOuterJoin(totalsByProd)
        val totalsWithMissingProds2 = totalsByProd.rightOuterJoin(products)
        // 결과가 동일함을 확인. 차이점은 Option 객체 위치가 다르다는 것.
        totalsWithMissingProds.collect()        // Array[(Int, (Array[String], Option[Double]))] = Array((34,(Array(34, GAM X360 Assassins Creed 3, 6363.95, 9),Some(62592.43000000001)))
        totalsWithMissingProds2.collect()       // Array[(Int, (Option[Double], Array[String]))] = Array((34,(Some(62592.43000000001),Array(34, GAM X360 Assassins Creed 3, 6363.95, 9)))

        /*
            Outer Join 변환 연산자의 결과에 Option 객체를 사용하는 이유는 NullPointerException을 피하기 위해서임.
            반면 Join 변환 연산자의 결과에는 Null 요소가 없기 때문에 Option 객체도 필요 없음.
            Option 객체 값은 None 객체일 수도 있고, Some 객체일 수도 있음.
            isEmpy 함수를 호출해 Option에 값이 존재하는지 확인할 수 있으며, get 함수로 Option 값을 가져올 수도 있음.
            또 두 함수를 연달아 호출하는 대신 getOrElse(default)를 사용할 수도 있음.
            getOrElse는 Option이 None일 때 default 표현식의 값을 그대로 반환하며, 값이 존재하면 get 결과를 반환함.
        */

        // 매출 데이터에 포함되지 않은 상품은 Option 값으로 None 객체를 전달함.
        // 따라서 어제 판매하지 않은 상품 목록을 얻으려면 None 값을 가진 요소만 남도록 결과 RDD를 필터링한 후 키와 None 객체를 제거하고 상품 데이터만 가져와야 함.
        // val missingProds = totalsWithMissingProds.filter(x => x._2._2 == None).map(x => x._2._1)
        val missingProds = totalsWithMissingProds.filter(x => x._2._2.isEmpty).map(x => x._2._1)
        missingProds.foreach(p => println(p.mkString(", ")))
        /*
            20, LEGO Elves, 4589.79, 4
            63, Pajamas, 8131.85, 3
            43, Tomb Raider PC, 2718.14, 1
            3, Cute baby doll, battery, 1808.79, 2
        */


        /*
            subtract, subtractByKey
                subtract은 첫 번째 RDD에서 두 번째 RDD의 요소를 제거한 여집합을 반환함.
                이 메서드는 일반 RDD에서도 사용할 수 있으며, 각 요소으 ㅣ키나 값만 비교하는 것이 아니라 요소 전체를 비교해 제거 여부 판단함.

                subtractByKey는 Pair RDD에서만 사용할 수 있는 메서드.
                첫 번째 RDD의 키-값 쌍 중에서 두 뻔쟤 RDD에 포함되지 않은 키의 요소들로 RDD를 구성해 반환함.
         */
        val missingProds2 = products.subtractByKey(totalsByProd).values
        missingProds2.foreach(p => println(p.mkString(", ")))


        /*
            cogroup
                cogroup 변환 연산자를 사용해 어제 판매한 상품과 판매하지 않은 상품의 목록을 한꺼번에 찾을 수 있음.
                cogroup은 여러 RDD 값을 키로 그룹핑하고, 각 RDD의 키별 값을 담은 Iterable 객체를 생성한 후 이 Iterable 객체 배열을 Pair RDD 값으로 반환함.
                즉 cogroup은 여러 RDD 값을 각각 키로 그룹핑한 후 키를 기준으로 조인함.
                cogroup은 RDD를 최대 세 개까지 조인할 수 있는데, 단 cogroup을 호출한 RDD와 cogroup에 전달된 RDD는 모두 동일한 타입의 키를 가져야 함.

                cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)]: RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))]
         */
        val prodTotCogroup = totalsByProd.cogroup(products)
        prodTotCogroup.filter(x => x._2._1.isEmpty).foreach(x => println(x._2._2.head.mkString(", ")))


        // intersection
        // 타입이 동일한 두 RDD에서 양쪽 모두에 포함된 공통 요소, 즉 교집합을 새로운 RDD로 반환함.
        totalsByProd.map(_._1).intersection(products.map(_._1)).take(5)         // Array[Int] = Array(34, 52, 96, 4, 16)


        // cartesian
        val rdd1 = sc.parallelize(List(7, 8, 9))
        val rdd2 = sc.parallelize(List(1, 2, 3))
        rdd1.cartesian(rdd2).collect()

        // cartesian 연산은 두 RDD 요소들을 서로 비교하는 데도 사용할 수 있음
        // 에) rdd2 숫자로 rdd1 숫자를 나머지 없이 나눌 수 있는 조합을 모두 찾을 수 있음
        // 예) 단기간에 비정상적으로 많은 상품을 구매한 고객 탐지 가능 -> tranData.cartesian(tranData)
        rdd1.cartesian(rdd2).filter(x => x._1 % x._2 == 0).collect()


        /*
            zip 변환 연산자로 RDD 조인
                zip과 zipPartitions 변환 연산자는 모든 RDD에 사용할 수 있음.
                RDD[T]의 zip 연산자에 RDD[U]를 전달하면 zip 연산자는 두 RDD의 각 요소를 순서대로 짝을 지은 새로운 RDD[(T, U)]를 반환함.

                하지만 스칼라 zip 함수와 달리 스파크의 zip 연산자는 두 RDD의 파티션 개수가 다르거나
                두 RDD의 모든 파티션이 서로 동일한 개수의 요소를 포함하지 않으면 오류를 발생시킴.
                한 RDD가 다른 RDD에 map 연산을 적용한 결과 RDD이면 zip 함수의 실행 조건을 만족시킬 수 있음.
                하지만 이처럼 까다로운 조건 때문에 스파크의 zip을 사용하기 다소 어려움.

                그러나 이 방법 외에는 zip 연산을 병렬로 실행하기가 쉽지 않음.
                스파크는 순차적인 연산에 적합하도록 설계되지 않았기 때문에 이런 엄격한 조건이 오히려 유용할 수 있음.

                반면 zipPartiitons 변환 연산자를 사용하면 모든 파티션이 서로 동일한 개수의 요소를 가져야 한다는 조건을 교묘하게 피할 수 있음.
                zipPartitions는 파티션에 포함된 요소를 반복문으로 처리한다는 점에서 mapPartitions와 유사하지만,
                여러 RDD의 파티션을 결합하는 데 사용한다는 점은 다름.
                모든 RDD는 파티션 개수가 동일해야 하지만, 파티션에 포함된 요소 개수가 반드시 같을 필요는 없음.

                zipPartitions는 인자 목록을 두개 받음.
                첫 번째는 zipPartitions로 결합할 RDD를 전달하며, 두 번째 목록에는 조인 함수를 정의해 전달함.
                조인 함수는 입력 RDD별로 각 파티션의 요소를 담은 Iterator 객체들을 받고 결과 RDD 값을 담은 새로운 Iterator를 반환해야 함.
                또 조인 함수를 구현할 때는 입력 RDD의 각 파티션에 포함된 요소 개수가 서로 다를 수 있다는 점을 고려해
                반복문이 각 Iterator 길이를 초과하지 않도록 신경 써야 함.

                또 zipPartitions 변환 연산자의 첫 번째 인자 목록에 preservesPartitiong이라는 선택 인수를 추가로 전달할 수 있음(기본값 : false)
                조인 함수가 데이터의 파티션을 보존한다고 판단했다면 이 인수를 true로 설정할 수 있음.
                반대로 이 인수를 false로 설정하면 결과 RDD의 Partitioner를 제거하며, 이후 다른 변환 연산자를 실행할 때 셔플링이 발생함.
         */
        val rdd3 = sc.parallelize(List(1, 2, 3))
        val rdd4 = sc.parallelize(List("n4", "n5", "n6"))
        rdd3.zip(rdd4).collect()

        val rdd5 = sc.parallelize(1 to 10, 10)
        val rdd6 = sc.parallelize((1 to 8).map(x => "n" + x), 10)
        /*
            rdd5.zipPrtitions(rdd6, preservcesPartitioning = true)((iter1, iter2) => {
                iter1.zipAll(iter2, -1, "empty").map({case(x1, x2) => x1 + "-" + x2})
            }).collect()
         */
        rdd5.zipPartitions(rdd6, preservesPartitioning = true)((iter1, iter2) => {
            // 스칼라 zipAll 함수를 사용해 두 Iterator 결합. zipAll은 컬렉션의 크기가 달라도 이들을 결합할 수 있음.
            iter1.zipAll(iter2, -1, "empty").map(x => x._1 + "-" + x._2)
        }).collect()        // Array[String] = Array(1-empty, 2-n1, 3-n2, 4-n3, 5-n4, 6-empty, 7-n5, 8-n6, 9-n7, 10-n8)

        /*
            전일 판매 실적 통계(각 고객이 구매한 상품의 평균, 최저 및 최고 가격과 구매 금액 합계)를 계산하려면 RDD 값과 결합 값을 병합할 때
            상품의 최저 가격과 최고 가격을 계산하고, 상품 구매 수량 및 구매 금액을 더해야 함.
            그런 다음 구매 금액 합계를 구매 수량 합계로 나누어 상품의 평균 가격을 계산함.
         */
        def createComb = (x: Array[String]) => {
            val total = x(5).toDouble
            val q = x(4).toInt
            (total / q, total / q, q, total)
        }

        def mergeVal: ((Double, Double, Int, Double), Array[String]) =>
            (Double, Double, Int, Double) = {
            case((mn, mx, c, tot), x) => {
                val total = x(5).toDouble
                val q = x(4).toInt
                (scala.math.min(mn, total / q), scala.math.max(mx, total / q), c + q, tot + total)
            }
        }

        def mergeComb: ((Double, Double, Int, Double), (Double, Double, Int, Double)) =>
            (Double, Double, Int, Double) = {
            case((mn1, mx1, c1, tot1), (mn2, mx2, c2, tot2)) =>
                (scala.math.min(mn1, mn2), scala.math.max(mx1, mx2), c1 + c2, tot1 + tot2)
        }

        val avgByCust = transByCust.combineByKey(createComb, mergeVal, mergeComb, new HashPartitioner(transByCust.partitions.length)).
            mapValues({case(mn, mx, cnt, tot) => (mn, mx, cnt, tot, tot / cnt)})

        avgByCust.first()       // (Int, (Double, Double, Int, Double, Double)) = (34,(3.942,2212.7425,82,77332.59,943.0803658536585))

        // CSV 형식으로 저장
        totalsAndProds.map(_._2).map(x => x._2.mkString(",") + x._1).saveAsTextFile("ch04output-totalsPerProd")
        avgByCust.map({case(id, (min, max, cnt, tot, avg)) =>
            "%d,%.2f,%.2f,%d,%.2f".format(id, min, max, cnt, tot, avg)
        }).saveAsTextFile("ch04output-avgByCust")
    }
}
