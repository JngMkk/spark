// 여러 RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object RDDsEx {
    def main(args: Array[String]) = {
        val sc = new SparkContext(new SparkConf().setMaster("local[*]"))

        /*
            PairRDD
                집계, 정렬, 데이터 조인과 같은 많은 사용 사례에 적합한 (키-값) 튜플로 구성된 RDD.
                키와 값은 정수, 문자열과 같은 단순 타입이거나 케이스 클래스, 배열, 리스트, 기타 타입의 컬렉션과 같은 복잡한 타입일 수 있음.
                (키-값) 기반의 확장 가능한 데이터 모델은 많은 장점을 제공하고 맵리듀스 패러다임의 기본 개념.

                RDD에 트랜스포메이션을 적용해 RDD를 (키-값) 쌍의 RDD로 쉽게 생성할 수 있음.
        */
        val statesPopRdd = sc.textFile("statesPopulation.csv")                                  // org.apache.spark.rdd.RDD[String] = statesPopulation.csv MapPartitionsRDD[7] at textFile at <console>:23
        statesPopRdd.first              // String = State,Year,Population
        statesPopRdd.take(5)            // Array[String] = Array(State,Year,Population, Alabama,2010,4785492, Alaska,2010,714031, Arizona,2010,6408312, Arkansas,2010,2921995)
        
        val pairRdd = statesPopRdd.map(record => (record.split(",")(0), record.split(",")(2)))  // org.apache.spark.rdd.RDD[(String, String)] = MapPartitionsRDD[8] at map at <console>:23
        pairRdd.take(10)                // Array[(String, String)] = Array((State,Population), (Alabama,4785492), (Alaska,714031), (Arizona,6408312), (Arkansas,2921995), (California,37332685), (Colorado,5048644), (Delaware,899816), (District of Columbia,605183), (Florida,18849098))


        /*
            DoubleRDD
                double 값의 모음으로 구성된 RDD. double 속성으로 인해 많은 통계 함수를 DoubleRDD와 함께 사용할 수 있음.
        */
        val rdd1 = sc.parallelize(Seq(1.0, 2.0, 3.0))                                           // org.apache.spark.rdd.RDD[Double] = ParallelCollectionRDD[9] at parallelize at <console>:2
        rdd1.mean                       // Double = 2.0
        rdd1.min                        // Double = 1.0
        rdd1.max                        // Double = 3.0
        rdd1.stdev                      // Double = 0.816496580927726


        /*
            SequenceFileRDD
                하둡 파일 시스템의 파일 포맷인 SequenceFile에서 생성됨.
                SequenceFile은 압축될 수 있고, 압축이 해제될 수 있음.

                맵리듀스 프로세스는 키와 값 쌍인 SequenceFile을 사용할 수 있음.
                키와 값은 Text, IntWritable 등과 같은 하둡 쓰기 가능한 데이터 타입임.
        */
        pairRdd.saveAsSequenceFile("seqfile")
        val seqRdd = sc.sequenceFile[String, String]("seqfile")                                 // org.apache.spark.rdd.RDD[(String, String)] = MapPartitionsRDD[14] at sequenceFile at <console>:23
        seqRdd.take(10)                 // Array[(String, String)] = Array((Nebraska,1868559), (Nevada,2786464), (New Hampshire,1322687), (New Jersey,8899162), (New Mexico,2085193), (New York,19673546), (North Carolina,9841590), (North Dakota,724019), (Ohio,11570022), (Oklahoma,3852415))


        /*
            CoGroupedRDD
                RDD의 부모와 함께 그룹핑되는 RDD.
                기본적으로 공통 키와 양 부모 RDD의 값 리스트로 구성된 pairRDD를 생성하기 때문에 양 부모 RDD는 pairRDD이어야 함.

                class CoGroupedRDD[K] extends RDD[(K, Array[Iterable[_]])]
        */
        val pairRdd2 = statesPopRdd.map(x => (x.split(",")(0), x.split(",")(1)))                // org.apache.spark.rdd.RDD[(String, String)] = MapPartitionsRDD[15] at map at <console>:23
        val coGroupRdd = pairRdd.cogroup(pairRdd2)                                              // org.apache.spark.rdd.RDD[(String, (Iterable[String], Iterable[String]))] = MapPartitionsRDD[18] at cogroup at <console>:24
        coGroupRdd.take(10)             // Array[(String, (Iterable[String], Iterable[String]))] = Array((Montana,(CompactBuffer(990641, 997821, 1005196, 1014314, 1022867, 1032073, 1042520),CompactBuffer(2010, 2011, 2012, 2013, 2014, 2015, 2016))), (California,(CompactBuffer(37332685, 37676861, 38011074, 38335203, 38680810, 38993940, 39250017),CompactBuffer(2010, 2011, 2012, 2013, 2014, 2015, 2016))), (Washington,(CompactBuffer(6743226, 6822520, 6895226, 6968006, 7054196, 7160290, 7288000),CompactBuffer(2010, 2011, 2012, 2013, 2014, 2015, 2016))), (Massachusetts,(CompactBuffer(6565524, 6611923, 6658008, 6706786, 6749911, 6784240, 6811779),CompactBuffer(2010, 2011, 2012, 2013, 2014, 2015, 2016))), (Kentucky,(CompactBuffer(4348662, 4369354, 4384799, 4400477, 4413057, 4424611, 4436974)...


        /*
            ShuffledRDD
                키 별로 RDD 엘리먼트를 섞어 동일한 익스큐터에서 동일한 키에 대한 값을 누적해 집계하거나 로직을 결합할 수 있음.

                class ShuffledRDD[K, V, C] extends RDD[(K, C)]
        */
        val pairRdd3 = statesPopRdd.map(x => (x.split(",")(0), 1))                              // org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[19] at map at <console>:23
        pairRdd3.take(5)                // Array[(String, Int)] = Array((State,1), (Alabama,1), (Alaska,1), (Arizona,1), (Arkansas,1))
        
        val shuffledRdd = pairRdd3.reduceByKey(_+_)                                             // org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[20] at reduceByKey at <console>:23
        shuffledRdd.take(5)             // Array[(String, Int)] = Array((Montana,7), (California,7), (Washington,7), (Massachusetts,7), (Kentucky,7))


        /*
            UnionRDD
                두 개의 RDD의 유니온 연산을 적용한 결과.

                class UnionRDD[T: ClassTag](sc: SparkContext, var rdds: Seq[RDD[T]]) extends RDD[T](sc, Nil)
        */
        val rdd1 = sc.parallelize(Seq(1, 2, 3))
        rdd1.take(5)

        val rdd2 = sc.parallelize(Seq(4, 5, 6))
        rdd2.take(5)

        val unionRdd = rdd1.union(rdd2) // org.apache.spark.rdd.RDD[Int] = UnionRDD[23] at union at <console>:24
        unionRdd.take(10)               // Array[Int] = Array(1, 2, 3, 4, 5, 6)


        /*
            HadoopRDD
                하둡 1.x 라이브러리의 맵리듀스 API를 사용해 HDFS에 저장된 데이터를 읽는 핵심 기능을 제공함.
                HadoopRDD는 기본적으로 제공되며, 모든 파일 시스템의 데이터를 RDD로 로드할 때 HadoopRDD를 볼 수 있음.

                class HadoopRDD[K, V] extends RDD[(K, V)]
        */


        /*
            NewHadoopRDD
                하둡 2.x 라이브러리의 새로운 맵리듀스 API를 사용해 HDFS, HBase 테이블, 아마존 S3에 저장된 데이터를 읽는 핵심 기능을 제공함.
                다양한 포맷으로 읽을 수 있기 때문에 여러 외부 시스템과 상호작용하기 위해 사용됨.

                class NewHadoopRDD[K, V](
                    sc: SparkContext,
                    inputFormatClass: Class[_ <: InputForamt[K, V]],
                    keyClass: Class[K],
                    valueClass: Class[V],
                    @transient private val _conf: Configuration
                ) extends RDD[(K, V)]

                가장 간단한 예는 SparkContext의 wholeTextFiles 함수를 사용해 wholeTextFileRDD를 생성하는 것.
        */
        val wholeRdd = sc.wholeTextFiles("wiki1.txt")           // org.apache.spark.rdd.RDD[(String, String)] = wiki1.txt MapPartitionsRDD[25] at wholeTextFiles at <console>:23
        wholeRdd.toDebugString                                  // String =
                                                                // (1) wiki1.txt MapPartitionsRDD[25] at wholeTextFiles at <console>:23 []
                                                                // |  WholeTextFileRDD[24] at wholeTextFiles at <console>:23 []
    }
}