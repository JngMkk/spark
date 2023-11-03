// case문은 정수를 문자열에 매핑하는 함수로 사용됨
object PatternMatching2 {
    def main(args: Array[String]): Unit = {
        println(comparison("two"))
        println(comparison("test"))
        println(comparison(1))
    }
    def comparison(x: Any): Any = x match {
        case 1 => "one"
        case "five" => 5
        case _ => "nothing else"
    }
}