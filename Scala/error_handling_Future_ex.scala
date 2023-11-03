// Future
// 스칼라에서는 단순히 넌블로킹 방식으로 태스크를 실행하고 싶고
// 태스크가 종료하면 태스크 결과를 처리할 방법이 필요하다면 Future를 제공함
// 예를 들어 병렬로 여러 웹 서비스를 호출하고 웹 서비스 호출 이후의 결과를 작업하고 싶을 때 사용

// 하나의 태스크를 실행하고 대기
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

// Exception in thread "main" java.util.concurrent.TimeoutException: Future timed out after [2 seconds]
object RunOneTaskbutBlock {
    def main(args: Array[String]) = {
        implicit val baseTime = System.currentTimeMillis

        val testFuture = Future {
            Thread.sleep(3000)
            2 + 2
        }

        val finalOutput = Await.result(testFuture, 2.seconds)
        println(finalOutput)
    }
}