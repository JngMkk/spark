import org.apache.spark._
import org.apache.spark.util.AccumulatorV2

// 사용자 정의 누적 변수 예
case class YearPopulation(year: Int, population: Long)

class StateAccumulator extends AccumulatorV2[YearPopulation, YearPopulation] {
    // 연도를 Int 타입으로, 인구를 Long 타입으로 하는 2개의 변수를 정의
    private var year = 0
    private var population: Long = 0L

    // 주와 인구가 0인지 여부를 확인하는 isZero를 리턴
    override def isZero: Boolean = year == 0 && population == 0L

    // 누적 변수를 복사하고 새로운 누적 변수 리턴
    override def copy(): StateAccumulator = {
        val newAcc = new StateAccumulator
        newAcc.year = this.year
        newAcc.population = this.population
        newAcc
    }

    // 주와 인구를 0으로 재설정
    override def reset(): Unit = {
        year = 0; population = 0L
    }

    // 값을 누적 변수에 추가
    override def add(v: YearPopulation): Unit = {
        year += v.year
        population += v.population
    }

    // 2개의 누적 변수 병합
    override def merge(other: AccumulatorV2[YearPopulation, YearPopulation]): Unit = {
        other match {
            case o: StateAccumulator => {
                year += o.year
                population += o.population
            }
            case _ =>
        }
    }

    // 누적 변수 값에 접근하기 위해 스파크에서 호출할 수 있는 함수
    override def value: YearPopulation = YearPopulation(year, population)
}

object AccumulatorEx {
    def main(args: Array[String]) = {
        val sc = new SparkContext(new SparkConf().setMaster("local[*]"))

        // 스파크 컨텍스트와 longAccumulator 함수를 사용해
        // 새로 생성한 누적 변수 변수를 0으로 초기화하는 Long 타입의 Accumulator를 생성하고 사용하는 예
        // 누적 변수가 맵 트랜스포메이션 내부에서 사용되면 누적 변수가 증가함.
        val statesPopRdd = sc.textFile("statesPopulation.csv")
        val acc1 = sc.longAccumulator("acc1")                       // org.apache.spark.util.LongAccumulator = LongAccumulator(id: 1, name: Some(acc1), value: 0)
        acc1.value                                                  // Long = 0
        val someRdd = statesPopRdd.map(x => {acc1.add(1); x})       // org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[14] at map at <console>:25
        someRdd.count                                               // Long = 351
        acc1.value                                                  // Long = 351
        acc1                                                        // org.apache.spark.util.LongAccumulator = LongAccumulator(id: 1, name: Some(acc1), value: 351)

        // 사용자 정의 누적 변수 SparkContext에 등록
        val statePopAcc = new StateAccumulator                      // StateAccumulator = Un-registered Accumulator: StateAccumulator
        sc.register(statePopAcc, "statePopAcc")

        val statesPopRdd2 = statesPopRdd.filter(_.split(",")(0) != "State")
        statesPopRdd2.count                                         // Long = 350
        statesPopRdd2.take(10)                                      // Array[String] = Array(Alabama,2010,4785492, Alaska,2010,714031, Arizona,2010,6408312, Arkansas,2010,2921995, California,2010,37332685, Colorado,2010,5048644, Delaware,2010,899816, District of Columbia,2010,605183, Florida,2010,18849098, Georgia,2010,9713521)

        statesPopRdd2.map(x => {
            val toks = x.split(",")
            val year = toks(1).toInt
            val pop = toks(2).toLong
            statePopAcc.add(YearPopulation(year, pop))
            x
        }).count                                                    // Long = 350

        statePopAcc                                                 // StateAccumulator = StateAccumulator(id: 0, name: Some(statePopAcc), value: YearPopulation(704550,2188669780))
    }
}