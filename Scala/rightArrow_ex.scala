// =>
// 이름으로 파라미터를 전달할 때 사용
// 파라미터에 접근할 때 표현식이 계산된다는 것을 의미
// 파라미터가 하나도 없는 함수 호출을 위한 사용상 편한 구문
// 예를 들면 x:() => Boolean의 형태

object UsingFatArrow {
    def fliesPerSecond(callback: () => Unit) = {
        while (true) {
            callback()
            Thread sleep 1000
        }
    }
    def main(args: Array[String]): Unit = {
        fliesPerSecond(() => println("Time and tide wait for none but fly like arrows..."))
    }
}