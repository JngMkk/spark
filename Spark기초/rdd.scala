/*
RDD 생성
    RDD는 스파크에서 사용되는 기본 객체, 데이터셋을 대표하는 불변 컬렉션
    신뢰성과 실패 복구 기능을 내장하고 있음
    본질적으로 RDD는 트랜스포메이션이나 액션과 같은 연산을 수행할 때 새로운 RDD를 생성함
    에러를 복구할 수 있도록 계보를 저장함
*/

/*
생성 방법
    - 컬렉션 병렬화
    - 외부 소스에서 데이터 로드
    - 기존 RDD의 트랜스포메이션
    - 스트리밍 API
*/

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object RDDEx {
    def main(args: Array[String]) = {
        // n개의 스레드를 가진 로컬 장비를 마스터로 지정하는 로컬 장비의 기본 셸
        val sc = new SparkContext(new SparkConf().setMaster("local[*]"))



        /*
        컬렉션 병렬화
            드라이버 프로그램에서 컬렉션에 parallelize 함수를 호출해 프로그램을 수행하는 것을 말함
            드라이버는 컬렉션을 병렬 처리하려 할 때 컬렉션을 파티션으로 분할하고 클러스터 전체에 데이터 파티션을 분배함
        */

        // SparkContext와 parallelize 함수를 사용해 일련의 숫자로 RDD를 생성하는 예
        val rdd_one = sc.parallelize(Seq(1, 2, 3))
        // org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0] at parallelize at <console>:23

        rdd_one.take(10)
        // Array[Int] = Array(1, 2, 3)



        /*
        외부 소스에서 데이터 로드
            아마존 S3, 카산드라, HDFS 등과 같은 외부 분산 소스에서 데이터를 읽는 것
            예를 들어 HDFS에서 RDD를 생성하면 HDFS의 분산 블록은 스파크 클러스터의 개별 노드에서 모두 읽음
            
            스파크 클러스터의 각 노드는 기본적으로 자체 입출력 작업을 수행하며,
            각 노드는 독립적으로 HDFS 블록에서 하나 이상의 블록을 읽음.
            일반적으로 스파크는 최대한 많은 데이터를 가능한 한 메모리에 RDD로 저장하기 위해 최선을 다함.
            스파크 클러스터의 노드가 반복적인 읽기 연산을 피할 수 있게 데이터를 캐싱함으로써 입출력 작업을 줄일 수 있음.
        */

        // SparkContext와 textFile 함수를 사용해 텍스트 파일에서 텍스트 라인을 로드하는 RDD
        // textFile 함수를 호출하면 자동으로 HadoopRDD를 사용해 여러 파티션 형태로 데이터를 탐색하고 로드한 후 클러스터 전체에 분산
        val rdd_two = sc.textFile("wiki1.txt")
        // org.apache.spark.rdd.RDD[String] = wiki1.txt MapPartitionsRDD[4] at textFile at <console>:23

        rdd_two.count
        // Long =9

        rdd_two.first
        // String = ...



        /*
        기존 RDD에 대한 트랜스포메이션
            RDD는 사실상 불변이므로 기존 RDD에 트랜스포메이션을 적용해 RDD를 생성할 수 있음.
            필터 함수는 트랜스포메이션의 대표적인 예.
        */

        // map 함수를 사용해 각 숫자에 2를 곱해 기존 RDD를 다른 RDD로 변환
        val rdd_one_x2 = rdd_one.map(i => i * 2)
        // org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[5] at map at <console>:23

        rdd_one_x2.take(10)
        // Array[Int] = Array(2, 4, 6)



        /*
        스트리밍 API
            스파크 스트리밍을 이용해 RDD를 생성할 수 있음.
            해당 RDD를 불연속 스트림 RDD라고 부름(Dstream RDD)
            ch9 참고
        */
    }
}