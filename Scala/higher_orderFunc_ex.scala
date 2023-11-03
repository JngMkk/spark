object higherOrderFunc {
    def main(args: Array[String]): Unit = {
        applyFuncOnRange(1, 10, quarterMaker)
        println()
        applyFuncOnRange(1, 10, addTwo)
    }
    def quarterMaker(value: Int): Double = value.toDouble / 4
    def addTwo(value: Int): Int = value + 2

    // 사용자 정의 함수를 전달하길 원한다면 사용자 정의 함수는 Int => AnyVal과 같은 시그니처를 가져야 함
    def applyFuncOnRange(begin: Int, end: Int, func: Int => AnyVal): Unit = {
        for (i <- begin to end) {
            println(func(i))
        }
    }
}