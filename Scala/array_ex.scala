object ArrayEx {
    def main(args: Array[String]) = {
        val numbers: Array[Int] = Array[Int](1, 2, 3, 4, 5, 1, 2, 3, 3, 4, 5)
        print("The full array is: ")
        for (i <- numbers) {
            print(i + " ")
        }
        println()

        println(numbers(2))

        var total = 0
        for (i <- 0 to (numbers.length - 1)) {
            total = total + numbers(i)
        }
        println("Sum = " + total)

        var min = numbers(4)
        for (i <- 0 to (numbers.length - 1)) {
            if (numbers(i) < min)
                min = numbers(i)
        }
        println("Min = " + min)

        var max = numbers(0)
        for (i <- 0 to (numbers.length - 1)) {
            if (numbers(i) > max)
                max = numbers(i)
        }
        println("Max = " + max)

        // Array.range 사용하여 배열 만들기
        // range 메소드와 concat 메소드를 바로 사용하려면
        // import Array._
        var myArray1 = Array.range(5, 20, 2)
        var myArray2 = Array.range(5, 20)

        for (x <- myArray1) {
            print(x + " ")
        }
        println()

        for (x <- myArray2) {
            print(x + " ")
        }
        println()

        var myArray3 = Array.concat(myArray1, myArray2)

        for (x <- myArray3) {
            print(x + " ")
        }
        println()

        // 행렬
        var myMatrix = Array.ofDim[Int](4, 4)

        for (i <- 0 to 3) {
            for (j <- 0 to 3) {
                myMatrix(i)(j) = j
            }
        }
        
        for (i <- 0 to 3) {
            for (j <- 0 to 3) {
                print(myMatrix(i)(j) + " ")
            }
            println()
        }
    }
}