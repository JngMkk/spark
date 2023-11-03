// for(var x <- range) {
//     statement(s)
// }

object UsingRangeWithForLoop {
    def main(args: Array[String]): Unit = {
        var i = 0
        for(i <- 1 to 10) {
            println("Value of i: " + i)
        }
    }
}