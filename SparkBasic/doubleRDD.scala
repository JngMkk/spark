/*
    Double 객체만 사용해 RDD를 구성하면 암시적 변환을 이용해 몇 가지 추가 함수를 쓸 수 있음.
    RDD는 데이터 타입에 따라 새로운 메서드를 자동으로 추가함.
    Double 객체만 포함하는 RDD는 org.apache.spark.rdd.DoubleRDDFunctions 클래스의 객체로 자동 변환됨.

    stats 연산자는 단 한번의 호출로 RDD 의 전체 개수, 합계, 평균, 최댓값, 최솟값, 분산, 표준편차 계산
    각 계산 결과는 stats 메서드가 반환하는 org.apache.spark.util.StatCounter 객체로 가져올 수 있음
    variance, stdev 연산자는 내부적으로 stats().variance와 stats().stdev를 호출함

    히스토그램 연산자도 있음. 히스토그램은 데이터를 시각화하는 데 주로 사용.
    히스토그램의 X축에는 데이터 값의 구간을 그리고 Y축에는 각 구간에 해당하는 데이터 밀도나 요소 개수 그림.
    histogram 연산자에는 버전이 두 가지 있음
        1. 구간 경계를 표현하는 Double 값의 배열을 받고, 각 구간에 속한 요소 개수를 담은 Array 객체를 반환
            구간 경계를 표현하는 Double 배열은 반드시 오름차순으로 정렬되어 있어야 하고,
            구간을 한 개 이상 표현할 수 있도록 하기 위해 두 개 이상의 요소를 포함해야 하며, 중복된 요소가 있으면 안 됨.
        2. 구간 개수를 받아 이것으로 입력 데이터의 전체 범위를 균등하게 나눈 후 요소 두 개로 구성된 튜플 하나를 결과로 반환.
            반환된 튜플의 첫 번째 요소는 구간 개수로 계산된 구간 경계의 배열이며, 두 번째 요소는 각 구간에 속한 요소 개수가 저장된 배열임.

    대규모 Double 데이터셋을 다룰 때는 통계 계산에 많은 시간을 소요할 가능성이 높음.
    이때는 sumApprox 또는 meanApprox 액션 연산자를 사용할 수 있음. 지정된 제한 시간 동안 근사 합계 또는 근사 평균을 계산함.
    sumApprox(timeout: Long, confidence: Double = 0.95): PartialResult[BoundedDouble]
    meanApprox(timeout: Long, confidence: Double = 0.95): PartialResult[BoundedDouble]
        sumApprox와 meanApprox 메서드는 밀리초 단위의 제한 시간을 인자로 받아 메서드가 실행될 최대 시간을 결정함.
        메서드는 제한 시간 안에 결과를 반환하지 못할 경우 제한 시간이 끝난 시점까지 계산한 중간 결과를 반환함.
        confidence 인자는 반환될 결과 값에 영향을 줌. sumApprox 연산자는 근사 합계 범위를 반환하고, meanApprox 연산자는 근사 평균 범위를 반환함.
        confidence 인자는 이 범위의 상한선과 하한선을 정규 분포(또는 t-분포)에 근거해 계산함.
        따라서 근사 합계(또는 평균)의 범위는 이 연산자가 계산한 합계(또는 평균)의 신뢰 구간이라 할 수 있음.

        sumApprox와 meanApprox 연산자는 finalValue 필드와 failure 필드로 구성된 PartialResult 객체를 결과로 반환함.
        failure 필드는 예외가 발생했을 때면 Exception 객체를 반환함.
        finalValue는 BoundedDouble 타입의 객체로 단일 결과 값이 아닌 값의 확률 범위, 평균값, 신뢰 수준을 제공함.
*/
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object DoubleRDDEx {
    def main(args: Array[String]) = {
        val sc = new SparkContext(new SparkConf().setMaster("local[*]"))

        val lines = sc.textFile("client-ids.txt")
        val ids = lines.flatMap(_.split(","))

        // 이 RDD는 Int 객체로 구성되지만 Double 객체로 자동 변환되므로 double RDD 함수가 암시적으로 적용됨.
        val intIds = ids.map(_.toInt)
        intIds.mean
        intIds.sum
        intIds.variance
        intIds.stdev

        intIds.histogram(Array(1.0, 50.0, 100.0))   // Array[Long] = Array(9, 5)
        intIds.histogram(3)                         // (Array[Double], Array[Long]) = (Array(15.0, 42.66666666666667, 70.33333333333334, 98.0),Array(9, 0, 5))
    }
}