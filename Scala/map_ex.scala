// 맵은 키와 값의 쌍으로 구성된 Iterable.
// 기본 데이터 타입을 유지하기 위해 사용될 수 있으므로 가장 널리 사용되는 커넥션 중 하나.
object MapEx {
    def main(args: Array[String]) = {
        println(Map(1 -> 2))
        println(Map("X" -> "Y"))

        // 스칼라의 Predef 객체는 pair(key, value)에 대한 대체 구문으로
        // key -> value를 쓸 수 있는 암시적 변환을 제공함
        // 아래 두 결과는 동일함.
        println(Map(2 -> "two", 4 -> "four"))
        println(Map((2, "two"), (4, "four")))

        // Map을 사용해 함수가 저장될 수 있음
        val myMax = Map("getMax" -> getMax())
        println("My max is: " + myMax)

        println(Map(1 -> Map("X" -> "Y")))

        val capitals = Map("Ireland" -> "Dublin", "Britain" -> "London", "Germany" -> "Berlin")
        val temp: Map[Int, Int] = Map()

        println("Keys in capitals: " + capitals.keys)
        println("Values in capitals: " + capitals.values)
        println("Check if capitals is empty: " + capitals.isEmpty)
        println("Check if temp is empty: " + temp.isEmpty)

        val capitals1 = Map("Ireland" -> "Dublin", "Turkey" -> "Ankara", "Egypt" -> "Cairo")
        val capitals2 = Map("Germany" -> "Berlin", "Saudi Arabia" -> "Riyadh")

        // ++ 연산자를 사용해 2개의 맵을 하나로 합침
        var capitalsConcatenated = capitals1 ++ capitals2
        println("capitals1 ++ capitals2: " + capitalsConcatenated)

        // ++ 연산자를 메소드로 사용해 2개의 맵을 하나로 합침
        capitalsConcatenated = capitals1.++(capitals2)
        println("capitals1.++(capitals2): " + capitalsConcatenated)

    }

    var myArr = Array.range(5, 20, 2)
    def getMax(): Int = {
        // 가장 큰 엘리먼트를 찾음
        var max = myArr(0)
        for (i <- 1 to (myArr.length - 1)) {
            if (myArr(i) > max)
                max = myArr(i)
        }
        max
    }
}