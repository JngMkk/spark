/*
    Window Func
        import org.apache.spark.sql.expressions.Window
            
        윈도우 함수를 사용하면 전체 데이터나 일부 필터링된 데이터 대신 데이터 윈도우에서 집계를 수행할 수 있음.
        누적 합계, 동일한 키에 대한 이전 값의 델타, 가중 이동 평균 등을 구할 때 사용

        윈도우를 지정하는 API는 세 가지 속성 paritionBy, orderBy, rowsBetween을 필요로 함.
        partitionBy는 지정한 파티션/그룹으로 데이터를 보냄.
        orderBy는 데이터의 각 파티션 내에서 데이터를 정렬하는 데 사용.
        rowsBetween은 계산을 수행할 윈도우 프레임이나 슬라이딩 윈도우의 범위를 지정함.

        - ntile
            윈도우에서 널리 사용되는 집계이며 일반적으로 입력 데이터셋을 n개의 부분집합으로 나눔.
            예를 들어 예측 분석에서는 10분위수를 사용해 데이터를 먼저 그룹핑한 다음,
            공평하게 데이터를 분배하기 위해 데이터를 10개의 부분으로 나눔.
            이는 윈도우 함수 접근 방식의 자연스러운 함수이기 때문에 ntiles는 윈도우 함수가 도움을 줄 수 있는 좋은 예.
*/
import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object WindowFuncEx {
    def main(args: Array[String]) = {
        val statesPopDF = spark.read.option("header", "true").option("inferschema", "true").option("sep", ",").csv("statesPopulation.csv")

        // Window Func
        val windowF = Window.partitionBy("State").orderBy(col("Population")).rowsBetween(Window.unboundedPreceding, Window.currentRow)

        statesPopDF.select(col("State"), col("Year"), col("Population"),
                            rank().over(windowF).alias("rank")).sort("State", "rank").show(7)
        /*
        +-------+----+----------+----+
        |  State|Year|Population|rank|
        +-------+----+----------+----+
        |Alabama|2011|   4779918|   1|
        |Alabama|2010|   4785492|   2|
        |Alabama|2012|   4815960|   3|
        |Alabama|2013|   4829479|   4|
        |Alabama|2014|   4843214|   5|
        |Alabama|2015|   4853875|   6|
        |Alabama|2016|   4863300|   7|
        +-------+----+----------+----+
        */


        // ntile
        statesPopDF.select(col("State"), col("Year"),
                            ntile(2).over(windowF).alias("ntile(2)"),
                            rank().over(windowF).alias("rank")).sort("State", "rank").show(14)
        /*
        +-------+----+--------+----+
        |  State|Year|ntile(2)|rank|
        +-------+----+--------+----+
        |Alabama|2011|       1|   1|
        |Alabama|2010|       1|   2|
        |Alabama|2012|       1|   3|
        |Alabama|2013|       1|   4|
        |Alabama|2014|       2|   5|
        |Alabama|2015|       2|   6|
        |Alabama|2016|       2|   7|
        | Alaska|2010|       1|   1|
        | Alaska|2011|       1|   2|
        | Alaska|2012|       1|   3|
        | Alaska|2014|       1|   4|
        | Alaska|2013|       2|   5|
        | Alaska|2015|       2|   6|
        | Alaska|2016|       2|   7|
        +-------+----+--------+----+
        */
    }
}