object higherOrderFunc3 {

    def testHOF(func: Int => String, value: Int) = func(value)
    def paramFunc[A](x: A) = "[" + x.toString() + "]"

    def main(args: Array[String]): Unit = {
        println(paramFunc(100))
        println(testHOF(paramFunc, 10))
    }
}