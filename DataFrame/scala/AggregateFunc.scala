/*
    집계
        집계는 조건을 기반으로 데이터를 수집하고 데이터를 분석하는 방법을 의미함.
        대부분의 사용 사례에서 원시 데이터 레코드만 갖고 분석하기에는 유용하지 않기 때문에 모든 크기의 데이터를 이해할 때 집계가 매우 중요하게 사용됨.

    - 집계 함수
        대부분의 집계는 org.apache.spark.sql.functions 패키지의 함수를 사용해 수행할 수 있음.
        또는 UDAF(User Defined Aggregation Function)라고 하는 사용자 정의 집계 함수로 집계를 생성할 수 있음.

        각 그룹핑 연산은 집계를 지정할 수 있는 RelationalGroupedDataSet을 리턴함.

        - count
            가장 기본적인 집계 함수. 지정된 칼럼의 로우 개수를 계산함.
            확장 API는 countDistinct, 중복 제거.
            
            def count(columnName: String): TypedColumn[Any, Long]
            def count(e: Column): Column
            def countDistinct(columnName: String, columnNames: String*): Column
            def countDistinct(expr: Column, exprs: Column*): Column

        - first
            RelationalGroupedDataSet의 첫 번째 레코드를 얻음

            def first(columnName: String): Column
            def first(e: Column): Column
            def first(columnName: String, ignoreNulls: Boolean): Column
            def first(e: Column, ignoreNulls: Boolean): Column
        
        - last
            RelationalGroupedDataSet의 마지막 레코드를 얻음
            함수 구조는 first와 같음
        
        - approx_count_distinct
            정확한 개수를 계산하는 방식보다 근사치 개수를 계산하는 방식이 훨씬 빠름.
            정확히 개수를 계산할 때는 대개 셔플링과 기타 연산이 많이 필요함.
            근사치가 100% 정확하지는 않지만 많은 사례에서 정확히 계산하지 않고도 똑같이 잘 수행될 수 있음.

            maximum relative standard deviation allowed (default = 0.05).
            For rsd < 0.01, it is more efficient to use :func:`countDistinct`

            def approx_count_distinct(columnName: String, rsd: Double): Column
            def approx_count_distinct(e: Column, rsd: Double): Column
            def approx_count_distinct(columnName: String): Column
            def approx_count_distinct(e: Column): Column
        
        - max: 특정 칼럼의 최대값
            def max(columnName: String): Column
            def max(e: Column): Column
        
        - average: 특정 칼럼의 평균
            함수 구조는 max와 같음
        
        - sum: 칼럼의 모든 값에 대한 총합
        - sumDistinct: 고유한 값 총합
            함수 구조는 max와 같음

        - variance: 개별 값에서 평균의 차를 제곱한 후의 평균
            - population variance
                def var_pop(columnName: String): Column
                def var_pop(e: Column): Column
            - unbiased variance
                def var_samp(column: String): Column
                def var_samp(e: Column): Column

        - standard deviation: 분산의 제곱근
            def stddev(columnName: String): Column
            def stddev(e: Column); Column
            
            - population standard deviation
                def stddev_pop(columnName: String): Column
                def stddev_pop(e: Column): Column
            - sample standard deviation
                def stddev_samp(columnName: String): Column
                def stddev_samp(e: Column): Column

        - kurtosis
            분포 형태의 차이를 계량화 하는 방법으로, 평균과 분산의 관점에서는 매우 유사하지만 실제로는 다름.
            이 경우 첨도는 분포의 중간과 비교해서 분포의 끝에서 분포의 가중치를 측정하는 좋은 척도가 됨.
            함수 구조는 max와 같음

        - skewness
            왜도는 평균 근처의 데이터 값에 대한 비대칭을 측정함
            함수 구조는 max와 같음

        - covariance
            두 랜덤 변수의 결합 가변성을 측정한 것.
            한 변수가 큰 값을 가지면 다른 변수에서도 큰 값을 가진다는 점에서 주로 일치함.
            그래서 변수가 더 작은 값을 가질 때도 동일하며, 두 변수는 유사한 동작을 보이고 공분산은 양수가 됨.
            이전 동작의 반대가 참이고 한 변수의 큰 값이 다른 변수의 더 작은 값과 연관된다면 공분산은 음수가 됨.

            - population covariance
                def covar_pop(columnName1: String, columnName2: String): Column
                def covar_pop(column: Column, column2: Column): Column
            - sample covariance
                def covar_samp(columnName1: String, columnName2: String): Column
                def covar_samp(column1: Column, column2: Column): Column

        - rollup: 계층이나 중첩 계산을 수행하는 데 사용되는 다차원 집계

        - cube
            rollup처럼 계층이나 중첩 계산을 수행하는 데 사용되는 다차원 집계지만
            큐브는 모든 차원에 동일한 연산을 수행한다는 차이점이 있음.
*/
import org.apache.spark._
import org.apache.spark.sql.functions._

object AggregateFunc {
    def main(args: Array[String]) = {
        val statesPopDF = spark.read.option("header", "true").option("inferschema", "true").option("sep", ",").csv("statesPopulation.csv")

        // count
        statesPopDF.select(col("*")).agg(count("State")).show
        statesPopDF.select(count("State")).show

        // countDistinct
        statesPopDF.select(col("*")).agg(countDistinct("State")).show
        statesPopDF.select(countDistinct("State")).show

        // first
        statesPopDF.select(first("State")).show

        // last
        statesPopDF.select(last("State")).show

        // approx_count_distinct
        statesPopDF.select(col("*")).agg(approx_count_distinct("State").alias("approxCountDistinct")).show
        statesPopDF.select(approx_count_distinct("State", 0.2).alias("approxCountDistinct")).show

        // max
        statesPopDF.select(max("Population")).show

        // average
        statesPopDF.select(avg("Population")).show

        // sum
        statesPopDF.select(sum("Population")).show

        // var
        statesPopDF.select(var_pop("Population")).show

        // stddev
        statesPopDF.select(stddev("Population")).show

        // kurtosis
        statesPopDF.select(kurtosis("Population")).show

        // skewness
        statesPopDF.select(skewness("Population")).show

        // covar
        statesPopDF.select(covar_pop("Year", "Population")).show

        // groupBy
        statesPopDF.groupBy("State").count.show(5)
        statesPopDF.groupBy("State").agg(min("Population"), avg("Population")).show(5)

        // rollup
        statesPopDF.rollup("State", "Year").count.orderBy("State").show(5)

        // cube
        statesPopDF.cube("State", "Year").count.orderBy("State").show(5)
    }
}