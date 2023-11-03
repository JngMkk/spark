/*
    Join
        기존 데이터베이스에서 조인은 더 완전한 뷰를 생성하기 위해 다른 테이블과 합치는 데 사용됨.
        
        두 데이터셋 간의 일반적인 조인은 왼쪽 데이터셋과 오른쪽 데이터셋에서 하나 이상의 키를 사용해 수행되고,
        키 셋에 대한 조건부 표현식을 Boolean 표현식으로 계산함.
        해당 Boolean 표현식의 결과가 true를 리턴하면 조인은 성공적이며,
        그렇지 않으면 조인된 데이터 프레임은 해당 조인이 포함되지 않음.

        join(right: dataset[_]): DataFrame                                                  조건 없는 내부 조인
        join(right: dataset[_], usingColumn: String)): DataFrame                            단일 컬럼과 내부 조인
        join(right: dataset[_], usingColumns: Seq[String]): DataFrame                       여러 칼럼과 내부 조인
        join(right: dataset[_], usingColumns: Seq[String], joinType: String): DataFrame     여러 칼럼과 조인 타입(내부, 외부, ...)으로 조인
        join(right: dataset[_], joinExprs: Column): DataFrame                               조인 표현식을 사용한 내부 조인
        join(right: dataset[_], joinExprs: Column, joinType: String): DataFrame             조인 표현식과 조인 타입(내부, 외부, ...)으로 조인

        ex)
            df1.join(df2, $"df1Key" === $"df2Key", "outer")


        - 조인의 내부 동작
            조인을 실행하면 다중 익스큐터를 사용해 데이터 프레임의 파티션에서 동작함.
            그러나 실제 작업과 성능은 조인의 타입과 조인되는 데이터셋의 특성에 따라 다름.

            - 셔플 조인
                두 개의 빅데이터셋 사이의 조인은 왼쪽 데이터셋과 오른쪽 데이터셋의 파티션이 전체 익스큐터로 분배되는 셔플 조인을 포함함.
                셔플은 비용이 많이 들고 파티션과 셔플 배포가 최적으로 수행되는지 확인하기 위해 로직을 분석하는 것이 중요함.

            - 브로드캐스트 조인
                큰 데이터셋과 이보다 작은 데이터셋 간의 조인은 큰 데이터셋의 파티션이 있는 모든 익스큐터에 작은 데이터셋이 브로드캐스트됨으로써 수행될 수 있음.

        - 조인 타입
            두 개의 데이터셋을 조인할 때 선택한 결정이 출력과 성능의 모든 차이를 만든다는 점에서 조인 타입은 중요함.

            - Inner Join
                각 로우를 왼쪽에서 오른쪽으로 비교하고, 양쪽 모두가 NULL 값이 아닐 때만 왼쪽 및 오른쪽 데이터셋에서 일치하는 로우 쌍을 조인함.
            - Cross Join
                왼쪽의 모든 로우와 오른쪽의 모든 로우를 카테시안 교차 곱으로 생성함.
            - Outer, Full, Full Outer Join
                왼쪽과 오른쪽의 모든 로우를 제공하며, 왼쪽 또는 오른쪽에 존재하면 NULL로 채움.
            - Left Anti Join
                오른쪽에 존재하지 않는 것을 기반으로 왼쪽의 로우만 제공.
            - Left, Left Outer Join
                왼쪽의 모든 로우와 왼쪽 및 오른쪽의 공통 로우를 추가함. 오른쪽에 없으면 NULL을 채움.
            - Left Semi Join
                오른쪽에 존재하는 값을 기반으로 왼쪽에 있는 로우만 제공함. 여기에 오른쪽 값은 포함되지 않음.
            - Right, Right Outer Join
                오른쪽의 모든 로우와 왼쪽 및 오른쪽의 공통 로우를 추가해 제공. 왼쪽에 로우가 없다면 NULL을 채움.

        - 조인의 성능 결과
            선택한 조인 타입에 따라 조인의 성능에 영향을 미침.
            이는 조인할 때 익스큐터에서 실행 파일 간의 데이터 셔플링을 필요로 하기 때문에
            다른 조인과 조인 순서까지 고려해야 할 필요가 있기 때문.

            - Inner Join
                왼쪽 테이블과 오른쪽 테이블에서 동일한 컬럼을 가져야 함.
                왼쪽 테이블이나 오른쪽 테이블의 키가 중복되거나 여러 복사본으로 있다면
                조인은 여러 키를 최소화하기 위해 올바르게 설계된 것보다
                완료하는 데 오래 걸리는 일종의 카테시안 조인으로 변환될 것임.

            - Cross Join
                모든 로우에서 왼쪽의 모든 로우와 오른쪽의 모든 로우를 일치시킴.
                교차 조인은 가장 좋지 않은 성능을 가진 조인이기 때문에 주의해서 사용해야 하며,
                특정 사용 사례에서만 사용해야 함.
            
            - Full Outer Join
                왼쪽과 오른쪽의 모든 로우를 제공하며, 값이 없으면 NULL로 채움.
                공통 로우가 거의 없는 테이블에서 사용하면 매우 큰 결과를 얻을 수 있고 성능이 저하될 수 있음.
            
            - Left Anti Join
                오른쪽에 존재하지 않는 것을 기반으로 왼쪽에 있는 로우만 제공.
                하나의 테이블만 확실히 고려하고 다른 테이블은 조인 조건만 확인하기 때문에 성능이 매우 좋음.
            
            - Left Outer Join
                왼쪽의 모든 로우와 왼쪽 및 오른쪽의 공통 로우를 제공함. 오른쪽에 없으면 NULL을 채움.
                공통 로우가 거의 없는 테이블에서 사용하면 성능이 저하될 수 있음.
            
            - Left Semi Join
                오른쪽에 존재하는 것을 기반으로 왼쪽의 로우만 제공함.
                하나의 테이블만 확실히 고려되고 다른 테이블은 조인 조건만 확인하기 때문에 성능이 매우 좋음.
*/
import org.apache.spark._
import org.apache.spark.sql.functions._
import spark.implicits._

object JoinEx {
    def main(args: Array[String]) = {
        val statesPopDF = spark.read.option("header", "true").option("inferschema", "true").option("sep", ",").csv("statesPopulation.csv")
        val statesTRDF = spark.read.option("header", "true").option("inferschema", "true").option("sep", ",").csv("statesTaxRates.csv")

        statesPopDF.count       // Long = 350
        statesTRDF.count        // Long = 47
        statesTRDF.show(5)
        statesPopDF.createOrReplaceTempView("statesPop")
        statesTRDF.createOrReplaceTempView("statesTR")

        // Inner Join (교집합)
        val joinDF = statesPopDF.join(statesTRDF, statesPopDF("State") === statesTRDF("State"), "inner")
        val joinSQL = spark.sql("SELECT * FROM statespop INNER JOIN statestr ON statespop.state = statestr.state")
        joinDF.count            // Long = 322
        joinSQL.count           // Long = 322
        joinDF.show(10)
        joinDF.explain
        /*
        == Physical Plan ==
        AdaptiveSparkPlan isFinalPlan=false
        +- BroadcastHashJoin [State#17], [State#40], Inner, BuildRight, false
        :- Filter isnotnull(State#17)
        :  +- FileScan csv [State#17,Year#18,Population#19] Batched: false, DataFilters: [isnotnull(State#17)],
            Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/jngmk/workspace/scala/ch8/statesPopulation.csv],
            PartitionFilters: [], PushedFilters: [IsNotNull(State)], ReadSchema: struct<State:string,Year:int,Population:int>
        +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [id=#561]
            +- Filter isnotnull(State#40)
                +- FileScan csv [State#40,TaxRate#41] Batched: false, DataFilters: [isnotnull(State#40)],
                Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/jngmk/workspace/scala/ch8/statesTaxRates.csv],
                PartitionFilters: [], PushedFilters: [IsNotNull(State)], ReadSchema: struct<State:string,TaxRate:double>
        */


        // Left Outer Join : 공통 로우 포함, 왼쪽 모든 로우 결과 얻음
        val joinDF = statesPopDF.join(statesTRDF, statesPopDF("State") === statesTRDF("State"), "leftouter")
        val joinSQL = spark.sql("SELECT * FROM statespop LEFT OUTER JOIN statestr ON statespop.state = statestr.state")
        joinDF.count            // Long = 350
        joinSQL.count           // Long = 350
        joinDF.show(10)

        // Right Outer Join
        val joinDF = statesPopDF.join(statesTRDF, statesPopDF("State") === statesTRDF("State"), "rightouter")
        val joinSQL = spark.sql("SELECT * FROM statespop RIGHT OUTER JOIN statestr ON statespop.state = statestr.state")
        joinDF.count            // Long = 323
        joinSQL.count           // Long = 323
        joinDF.show(10)

        // Full Outer Join
        val joinDF = statesPopDF.join(statesTRDF, statesPopDF("State") === statesTRDF("State"), "fullouter")
        val joinSQL = spark.sql("SELECT * FROM statespop FULL OUTER JOIN statestr ON statespop.state = statestr.state")
        joinDF.count            // Long = 351
        joinSQL.count           // Long = 351
        joinDF.show(10)

        // Left Anti Join ( A - B ) : 왼쪽만 표현됨.
        val joinDF = statesPopDF.join(statesTRDF, statesPopDF("State") === statesTRDF("State"), "leftanti")
        val joinSQL = spark.sql("SELECT * FROM statespop LEFT ANTI JOIN statestr ON statespop.state = statestr.state")
        joinDF.count            // Long = 28
        joinSQL.count           // Long = 28
        joinDF.show(10)

        // Left Semi Join ( Inner Join에서 왼쪽만 표현 )
        val joinDF = statesPopDF.join(statesTRDF, statesPopDF("State") === statesTRDF("State"), "leftsemi")
        val joinSQL = spark.sql("SELECT * FROM statespop LEFT SEMI JOIN statestr ON statespop.state = statestr.state")
        joinDF.count            // Long = 322
        joinSQL.count           // Long = 322
        joinDF.show(10)

        // Cross Join
        val joinDF = statesPopDF.crossJoin(statesTRDF)
        val joinSQL = spark.sql("SELECT * FROM statespop CROSS JOIN statestr")
        joinDF.count            // Long = 16450
        joinSQL.count           // Long = 16450
        joinDF.show(10)
    }
}