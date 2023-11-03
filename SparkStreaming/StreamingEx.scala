import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

import java.sql.Timestamp
import java.text.SimpleDateFormat

/*
    한 증권사에서 대시보드 애플리케이션을 구축해 달라는 의뢰를 받음.
    이 증권사 고객은 인터넷 애플리케이션을 사용해 거래 주문을 요청하며, 증권사 담당자는 고객의 주문을 받아 증권 시장에서 실제 거래를 진행함.
    구축할 대시보드 애플리케이션은 초당 거래 주문 건수, 누적 거래액이 가장 많은 고객 1~5위,
    지난 1시간 동안 거래량이 가장 많았던 유가 증권 1~5위를 집계해야 함.
 */

/*
    예제 데이터(50만 건)
        - 주문 시각: yyyy-MM-dd HH:mm:ss 형씪
        - 주문 ID: 순차적으로 증가시킨 정수
        - 고객 ID: 1에서 100 사이 무작위 정수
        - 주식 종목 코드: 80개의 주식 종목 코드 목록에서 무작위로 선택한 값
        - 주문 수량: 1에서 1000 사이 무작위 정수
        - 매매 가격: 1에서 100 사이 무작위 실수
        - 주문 유형: 매수 주문(B) 또는 매도 주문(S)
 */

object StreamingEx {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().master("local[*]").appName("Streaming Ex").getOrCreate()
        val sc = spark.sparkContext

        /*
            val conf = new SparkConf().setMaster("local[*]").setAppName("Streaming Ex")
            val ssc = new StreamingContext(conf, Seconds(5))
         */
        val ssc = new StreamingContext(sc, Seconds(5))

        /*
        이산 스트림 생성
            예시에서 사용할 파일은 50만 건의 주문인데, 50만 건의 주문이 한꺼번에 시스템으로 유입되는 것은 다소 비현실적이므로
            간단한 리눅스 셸 스크립트를 사용해 스트리밍 데이터를 생성함.
            이 스크립트는 압축을 해제한 데이터 파일을 파일 50개로 분할한 후, 분할한 파일을 HDFS 디렉터리에 3초 주기로 하나씩 복사함.

            셸 스크립트가 파일을 HDFS로 복사하려면 리눅스 사용자 계정에 hdfs 명령을 실행할 권한을 부여해야 함.
            HDFS에 접근할 수 있는 권한이 없다면 스크립트에 local 인수를 추가해 로컬 파일 시스템의 폴더를 지정할 수 있음.
         */
        // 먼저 소스 파일을 복사할 폴더를 고름(HDFS 또는 로컬)
        val homeDir = System.getenv("HOME")
        val filestream = ssc.textFileStream(homeDir + "/workspace/sourceCode/first-edition/ch06/data/")

        case class Order(time: Timestamp, orderId: Long, clientId: Int,
                         symbol: String, amount: Int, price: Double, buy: Boolean)

        // 데이터 파싱 후 Order 객체로 구성된 DStream 생성
        // DStream의 모든 RDD를 각 요소별로 처리하는 flatMap 변환 연산자 사용.
        // 포맷이 맞지 않는 데이터를 건너 뛰기 위해.
        // flatMap에 전달할 매핑 함수는 데이터를  포맷대로 파싱할 수 있으면 해당 데이터를 리스트에 담아 반환하고,
        // 파싱이 실패하면 빈 리스트를 반환함
        val orders = filestream.flatMap(line => {
            val dateForamt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val s = line.split(",")
            try {
                // assert : Tests an expression, throwing an AssertionError if false.
                assert(s(6) == "B" || s(6) == "S")
                List(Order(new Timestamp(dateForamt.parse(s(0)).getTime),
                    s(1).toLong, s(2).toInt, s(3), s(4).toInt, s(5).toDouble, s(6) == "B"))
            }
            catch {
                // 파싱 중 오류가 발생하면 해당 데이터와 오류 내용을 로그에 기록함 (단순히 System.out에만 출력)
                case e: Throwable => println("Wrong line format ("+e+"): " + line)
                // 파싱에 실패하면 빈 리스트를 반환해 해당 줄 건너뜀.
                List()
            }
        })

        // 거래 건수 집계
        // (Boolean, Long) 튜플이 최대 두 개 저장됨. 하나는 true(매수 주문) 나머지 하나는 false(매도 주문)
        val numPerType = orders.map(x => (x.buy, 1L)).reduceByKey((v1, v2) => v1 + v2)

        // 누적 거래액 집계
        val amountPerClient = orders.map(x => (x.clientId, x.amount * x.price))

        /*
        val amountState = amountPerClient.updateStateByKey(
            (vals, totalOpt: Option[Double]) => {
                totalOpt match {
                    // 이 키의 상태가 이미 존재할 때는 상태 값에 새로 유입된 값의 합계를 더함
                    case Some(total) => Some(vals.sum + total)
                    // 이전 상태 값이 없을 때는 새로 유입된 값의 합계만 반환
                    case None => Some(vals.sum)
                }
            }
        )
         */

        val updateAmountState =
            (clientId: Int, amount: Option[Double], state: State[Double]) => {
                // 새로 유입된 값을 새로운 상태 값으로 설정. 유입된 값이 없으면 0으로 설정
                var total = amount.getOrElse(0.toDouble)
                if (state.exists()) {
                    // 새로운 상태 값에 기존 상태 값을 더함
                    total += state.get()
                }
                // 새로운 상태 값으로 상태를 갱신
                state.update(total)
                // 고객 ID와 새 상태 값을 2-요소 튜플로 만들어 반환
                Some((clientId, total))
            }

        // stateSanpshots 메서드를 호출하지 않을 경우
        // mapWithState 메서드의 결과에는 현재 미니배치의 주기 사이에 거래를 주문했던 고객 ID와 누적 거래액만 포함됨.
        // stateSnapshots 메서드는 updateStateByKey와 마찬가지로 전체 상태(즉, 모든 고객 ID)를 포함한 DStream을 반환함.
        val amountState = amountPerClient.mapWithState(
            StateSpec.function(updateAmountState)
        ).stateSnapshots()

        // 거래액 1~5위 고객을 계산하려면 먼저 DStream의 각 RDD를 정렬한 후 각 RDD에서 상위 다섯 개 요소만 남겨야 함.
        val top5cli = amountState.transform(_.sortBy(_._2, ascending = false).
            map(_._1).zipWithIndex().
            filter(x => x._2 < 5))

        // 지난 1시간 동안 거래량이 가장 많았던 유가 증권 1~5위를 집계 (윈도 길이도 당연히 1시간이 되어야 함)
        // 이동 거리는 거래량 상위 1~5위 유가 증권과 다른 지표들을 각 미니배치마다 계산해야 하므로 미니배치 주기와 동일하게 설정
        val stocksPerWindow = orders.
            map(x => (x.symbol, x.amount)).
            reduceByKeyAndWindow((a1: Int, a2: Int) => a1 + a2, Minutes(60))

        val topStocks = stocksPerWindow.transform(_.sortBy(_._2, ascending = false).map(_._1).
            zipWithIndex().filter(x => x._2 < 5)).repartition(1).
            map(x => x._1).glom().map(arr => ("TOP5STOCKS", arr.toList))

        // DStream 병합
        // union(병합하려면 요소 타입이 서로 동일해야 함)
        // 두 번째 요소가 문자열 리스트인 이유는 거래액 1~5위 고객의 목록이 리스트 타입이기 때문
        val buySellList = numPerType.map(x =>
            if(x._1) ("BUYS", List(x._2.toString))
            else ("SELLS", List(x._2.toString))
        )

        // top5cli DStream을 변환하려면 먼저 repartition(1)을 호출해 데이터를 단일 파티션으로 모아야 함.
        // 이어서 누적 거래액을 제거하고 고객 ID를 문자열 타입으로 변환한 후 glom을 호출해
        // 파티션에 포함된 모든 고객ID를 단일 배열로 그룹핑함. 마지막으로 지표 이름과 고객 ID의 배열을 튜플로 매핑함
        val top5cliList = top5cli.repartition(1).
            map(x => x._1.toString).
            glom().
            map(arr => ("TOP5CLIENTS", arr.toList))

        val finalStream = buySellList.union(top5cliList).union(topStocks)

        /*
        결과를 파일로 저장
            saveAsTextFiles
                접두 문자열(필수)과 접미 문자열(선택)을 받아 데이터를 주기적으로 저장할 경로를 구성함.
                접두 문자열과 접미 문자열을 모두 전달하면 각 미니배치 RDD의 데이터는 <접두 문자열>-<밀리초 단위 시각>.<접미 문자열> 폴더에 저장됨.
                반면 접두 문자열만 전달하면 <접두 문자열>-<밀리초 단위 시각>을 경로로 사용.
                즉 미니배치 RDD의 주기마다 새로운 디렉터리를 생성한다는 의미.
                스파크 스트리밍은 이 디렉터리 아래에 미니배치 RDD의 각 파티션을 part-xxxxx라는 파일로 저장함(xxxxx는 파티션 번호)

                RDD 폴더별로 part-xxxxx 파일을 하나씩만 생성하려면 데이터를 저장하기 전에 DStream의 파티션을 하나로 줄여야 함.
         */
        finalStream.repartition(1).saveAsTextFiles(
            homeDir + "/workspace/sourceCode/first-edition/ch06/data/streaming-output/output", "txt"
        )

        // 체크포인팅 디렉터리 지정
        // updateStateByKey 메서드를 사용할 때는 메서드가 반환하는 State DStream에 체크포인팅을 반드시 적용해야 함.
        // 이는 updateStateByKey가 각 미니배치마다 RDD의 DAG를 계속 연장하면서 스택 오버플로우 오류를 유발하기 때문임.
        // RDD를 주기적으로 체크포인트에 저장하면 RDD의 DAG가 과거의 미니배치에 의존하지 않도록 만들 수 있음.
        sc.setCheckpointDir(homeDir + "/workspace/sourceCode/first-edition/ch06/data/checkpoint")

        // 스트리밍 시작 & 스파크 애플리케이션 대기
        ssc.start()
        ssc.awaitTermination()

        // 출력 결과
        val allCounts = sc.textFile(
            homeDir + "/workspace/sourceCode/first-edition/ch06/data/streaming-output/output*.txt"
        )
        allCounts.take(5)
    }
}
