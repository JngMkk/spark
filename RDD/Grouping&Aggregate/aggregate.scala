/*
    집계
        집계 기법을 사용하면 임의의 방식으로 RDD의 엘리먼트를 병합해 계산을 수행할 수 있음. 실제로 집계는 대규모 데이터 분석에서 가장 중요함.

        - groupByKey
            RDD의 각 키 값을 하나의 시퀀스로 그룹핑함. 또한 파티셔너를 전달해서 결과로 얻는 (키-값) 쌍 RDD 파티셔닝을 제어할 수 있음.
            파티셔너는 기본적으로 HashPartitioner가 사용되지만, 사용자 정의 파티셔너를 파라미터로 제공할 수 있음.
            각 그룹 내의 엘리먼트 순서는 보장되지 않으며, 결과 RDD는 계산될 때마다 다를 수 있음.

            모든 데이터 셔플링을 진행하기 때문에 비싼 연산이라 할 수 있음.
            reduceByKey 또는 aggregateByKey가 더 나은 성능을 제공함.

            def groupByKey(partitioner: Partitioner): RDD[(K, Iterable[V])]
            def groupByKey(numPartitions: Int): RDD[(K, Iterable[V])]

            groupByKey는 메모리의 모든 키에 대해 모든 키-값 쌍을 보유할 수 있어야 함.
            키에 너무 많은 값을 포함하면 OutOfMemoryError가 발생할 수 있음.

            파티션의 모든 엘리먼트를 파티셔너를 기반으로 하는 파티션에 보내 동일한 키에 대한 모든 키-값 쌍을 동일한 파티션에서 수집함.
            해당 groupByKey가 완료되면 쉽게 집계 연산을 수행할 수 있음.


        - reduceByKey
            pairRDD의 모든 엘리먼트를 셔플링해 전송하지 않고 로컬 컴바이너를 사용해 먼저 로컬에서 일부 기본 집계를 수행한 다음 결과 엘리먼트를 전송시켜 성능을 향상시킴.
            모든 데이터를 전송할 필요가 없기 때문에 전송 데이터가 크게 줄어듬.
            교환 및 결합 reduce 함수를 사용해 각 키의 값을 병합함.
            물론 우선 결과를 리듀서에 보내기 전에 각 매퍼에서 병합을 수행할 것임.

            def reduceByKey(partitioner: Partitioner, f: (V, V) => V): RDD[(K, V)]
            def reduceByKey(f: (V, V) => V, numpartitions: Int): RDD[(K, V)]
            def reduceByKey(f: (V, V) => V): RDD[(K, V)]

            파티션의 모든 엘리먼트를 파티셔너를 기반으로 하는 파티션에 보내 동일한 키에 대한 (키-값)의 모든 쌍이 동일한 파티션에서 수집됨.
            그러나 셔플링이 시작되기 전에 로컬 집계는 수행되기 때문에 셔플링될 데이터가 줄어듦.
            해당 태스크가 완료되면 마지막 파티션에서 집계 연산이 쉽게 수행될 수 있음.


        - aggregateByKey
            reduceByKey와 매우 유사. aggregateByKey를 사용하면 파티션 집계가 유연하고 사용자 정의가 가능함.
            그리고 단일 함수 호출에서 각 주별 전체 인구뿐 아니라 특정 함수 호출에서 모든 <Year, Population> 튜플에 대한 리스트를 생성하는 것과 같은 파티션 간에 복잡한 사용자 사례도 허용함.

            주어진 컴바이너 함수와 초깃값(또는 0)을 사용해 각 키의 값을 모아서 집계할 수 있음.

            다른 집계 함수와 가장 큰 차이점은 바로 RDD V 값에 대한 타입이 아닌 결과 타입 U를 리턴할 수 있음.
            따라서 V를 U로 병합하는 단일 태스크와 두 개의 U를 병합하는 단일 태스크가 필요함.
            전자는 파티션에서 값을 병합하는 데 사용되고, 후자는 파티션 간에 값을 병합하는 데 사용됨.
            메모리 할당을 피하기 위해 이 두 함수는 새로운 U를 생성하는 대신 첫 번째 파라미터를 수정하고 리턴할 수 있음.

            def aggregateByKey[U: ClassTag](zeroValue: U, partitioner: Partitioner)(seqOP: (U, V) => U, combOp: (U, U) => U): RDD[(K, U)]
            def aggregateByKey[U: ClassTag](zeroValue: U, numPartitions: Int)(seqOp: (U, V) => U, combOp: (U, U) => U): RDD[(K, U)]
            def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U, combOp: (U, U) => U): RDD[(K, U)]

            개별 파티션의 모든 엘리먼트를 대상으로 파티션 별로 집계를 수행한 후 파티션을 병합할 때는 다른 집계 로직을 수행함.
            궁극적으로 동일 키를 같는 모든 (키-값) 쌍이 동일 파티션으로 수집됨.
            그러나 aggregateByKey는 groupByKey와 reduceByKey의 경우처럼 집계 수행 방식과 생성된 출력에 대한 집계 방식이 고정 돼 있지 않아 더 유연하고 사용자 정의가 가능함.

        
        - combineByKey
            aggregateByKey와 매우 유사함.
            combineByKey는 combineByKeyWithClassTag를 내부적으로 호출함. 이는 aggregateByKey와 동일함.
            aggregateByKey처럼 각 파티션에서 연산을 적용한 다음 컴바이너 간에 태스크를 적용함.

            combineByKey는 RDD[K, V]를 RDD[K, C]로 바꿈.
            여기서 C는 이름 키 K로 수집되거나 병합된 V 리스트임.

            combineByKey를 호출할 때 사용할 수 있는 세 가지 함수가 있음
                - createCombiner : V를 C로 벼노한하고 하나의 엘리먼트 리스트로 생성
                - mergeValue : C 리스트의 끝에 V를 추가해 V를 C에 병합
                - mergeCombiners : 두 개의 C를 하나로 병합

            def combineByKey[C](createCombiner: V => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C, numPartitions: Int): RDD[(K, C)]
            def combineByKey[C](createCombiner: V => C, mergeValue: (C, V) => C, mergeCombiners: (C, C) => C, partitioner: Partitioner, mapSideCombine: Boolean = true, serializer: Serializer = null): RDD[(K, C)]

            개별 파티션의 모든 엘리먼트에서 동작 중인 파티션에서 집계를 수행한 후 파티션을 병합할 때 다른 집계 로직을 적용함.
            궁극적으로 동일한 키에 대한 모든 (키-값) 쌍은 동일 파티션에 수집됨.
            그러나 combineByKey를 사용할 때는 집계 생성 방식과 생성된 출력에 대한 집계가 groupByKey와 reduceByKey처럼 고정돼 있지 않아 더 유연하고 사용자 정의가 가능함.

        
        비교
            groupByKey는 키의 해시 코드를 생성한 다음, 데이터를 셔플링해 동일한 파티션의 각 키에 대한 값을 수집함으로써 pairRDD의 HashPartitioning을 수행함.
            많은 셔플링을 발생시킴.

            reduceByKey는 groupByKey와 비교해 셔플링 스테이지에서 전송된 데이터를 최소화 하기 위해 로컬 컴바이너 로직을 사용해 성능을 향상시킴.
            groupByKey와 결과는 같지만 훨씬 성능이 좋음.

            aggregateByKey의 동작 방식은 reduceByKey와 매우 비슷하지만 groupByKey와 reduceByKey보다 강력한 기능을 가지고 있음.
            동일 데이터 타입에서 동작할 필요가 없고 파티션에서 다른 집계를 수행하고 파티션 간에 다른 집계를 수행할 수 있음.

            combineByKey는 컴바이너를 생성하는 초기 함수를 제외하고 aggregateByKey와 성능이 매우 비슷함.
*/

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object AggregateEx {
    def main(args: Array[String]) = {
        val sc = new SparkContext(new SparkConf().setMaster("local[*]"))

        // RDD 초기화
        // header 제거
        val statesPopRdd = sc.textFile("statesPopulation.csv").filter(_.split(",")(0) != "State")           // org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[3] at filter at <console>:25
        statesPopRdd.take(10)               // Array[String] = Array(Alabama,2010,4785492, Alaska,2010,714031, Arizona,2010,6408312, Arkansas,2010,2921995, California,2010,37332685, Colorado,2010,5048644, Delaware,2010,899816, District of Columbia,2010,605183, Florida,2010,18849098, Georgia,2010,9713521)

        // pariRDD로 변환
        val pairRdd = statesPopRdd.map(x => x.split(",")).map(y => (y(0), (y(1).toInt, y(2).toInt)))        // org.apache.spark.rdd.RDD[(String, (Int, Int))] = MapPartitionsRDD[5] at map at <console>:25
        pairRdd.take(10)                    // Array[(String, (Int, Int))] = Array((Alabama,(2010,4785492)), (Alaska,(2010,714031)), (Arizona,(2010,6408312)), (Arkansas,(2010,2921995)), (California,(2010,37332685)), (Colorado,(2010,5048644)), (Delaware,(2010,899816)), (District of Columbia,(2010,605183)), (Florida,(2010,18849098)), (Georgia,(2010,9713521)))

        // groupByKey 사용
        val groupedRdd = pairRdd.groupByKey.map(x => {var sum=0; x._2.foreach(sum += _._2); (x._1, sum)})   // org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[7] at map at <console>:26
        groupedRdd.take(10)                 // Array[(String, Int)] = Array((Montana,7105432), (California,268280590), (Washington,48931464), (Massachusetts,46888171), (Kentucky,30777934), (Pennsylvania,89376524), (Georgia,70021737), (Tennessee,45494345), (North Carolina,68914016), (Utah,20333580))

        // reduceByKey 사용
        val reduceRdd = pairRdd.reduceByKey((x, y) => (x._1, x._2 + y._2)).map(x => (x._1, x._2._2))        // org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[9] at map at <console>:25
        reduceRdd.take(10)                  // Array[(String, Int)] = Array((Montana,7105432), (California,268280590), (Washington,48931464), (Massachusetts,46888171), (Kentucky,30777934), (Pennsylvania,89376524), (Georgia,70021737), (Tennessee,45494345), (North Carolina,68914016), (Utah,20333580))

        // aggregateByKey 사용
        val initialSet = 0
        val addToSet = (s: Int, v: (Int, Int)) => s + v._2                                                  // (Int, (Int, Int)) => Int = $Lambda$3111/0x000000084108b040@23f13b32
        val mergePartitionsSets = (p1: Int, p2: Int) => p1 + p2                                             // (Int, Int) => Int = $Lambda$3113/0x000000084108f040@10125936
        val aggregatedRdd = pairRdd.aggregateByKey(initialSet)(addToSet, mergePartitionsSets)               // org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[10] at aggregateByKey at <console>:28
        aggregatedRdd.take(10)              // Array[(String, Int)] = Array((Montana,7105432), (California,268280590), (Washington,48931464), (Massachusetts,46888171), (Kentucky,30777934), (Pennsylvania,89376524), (Georgia,70021737), (Tennessee,45494345), (North Carolina,68914016), (Utah,20333580))

        // combineByKey 사용
        val createCombiner = (x: (Int, Int)) => x._2                                                        // ((Int, Int)) => Int = $Lambda$3153/0x000000084110e840@569639ef
        val mergeValues = (c: Int, x: (Int, Int)) => c + x._2                                               // (Int, (Int, Int)) => Int = $Lambda$3154/0x000000084110f040@4e82fd8a
        val mergeCombiners = (c1: Int, c2: Int) => c1 + c2                                                  // (Int, Int) => Int = $Lambda$3155/0x0000000841118040@4da95065
        val combinedRdd = pairRdd.combineByKey(createCombiner, mergeValues, mergeCombiners)                 // org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[11] at combineByKey at <console>:28
        combinedRdd.take(10)                // Array[(String, Int)] = Array((Montana,7105432), (California,268280590), (Washington,48931464), (Massachusetts,46888171), (Kentucky,30777934), (Pennsylvania,89376524), (Georgia,70021737), (Tennessee,45494345), (North Carolina,68914016), (Utah,20333580))
    }
}