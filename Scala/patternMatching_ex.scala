// 패턴 매칭

object PatternMatching1 {
    def main(args: Array[String]): Unit = {
        println(matchInteger(3))
    }
    def matchInteger(x: Int): String = x match {
        case 1 => "one"
        case 2 => "two"
        case _ => "greater than two"
    }
}