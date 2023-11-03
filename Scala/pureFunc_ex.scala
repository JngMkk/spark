object PureAndNonPureFunc {
    def pureFunc(cityName: String) = s"I live in $cityName"
    def notpureFunc(cityName: String) = println(s"I live in $cityName")
    def pureMul(x: Int, y: Int) = x * y
    def notpureMul(x: Int, y: Int) = println(x * y)

    def main(args: Array[String]): Unit = { 
        pureFunc("Seoul")
        notpureFunc("Namyangju")
        pureMul(10, 25)
        notpureMul(10, 25)

        val data = Seq.range(1, 10).reduce(pureMul)
        println(s"My sequence is: " + data)
    }
}