object SetEx {
    def main(args: Array[String]) = {
        // HashSet(5, 1, 2, 3, 4)
        val numbers = Set(1, 2, 3, 4, 5, 1, 2, 3, 4, 5)

        // 정수 타입의 빈 셋
        var sInt: Set[Int] = Set()

        // 짝수 셋
        var sEven: Set[Int] = Set(2, 4, 8, 10)

        // 이 구문도 사용 가능
        var sEven2 = Set(2, 4, 8, 10)

        val cities = Set("Dublin", "London", "NY")
        val tempNums: Set[Int] = Set()

        // head, tail을 찾고 셋이 비어 있다면 확인
        println("Head of cities: " + cities.head)
        println("Tail of cities: " + cities.tail)
        println("Check if cities is empty: " + cities.isEmpty)
        println("Check if tempNums is empty: " + tempNums.isEmpty)

        val citiesEurope = Set("Dublin", "London", "NY")
        val citiesTurkey = Set("Istanbul", "Ankara")

        // ++ 연산자를 사용해 셋을 하나로 합침
        var citiesConcatenated = citiesEurope ++ citiesTurkey
        println("citiesEurope ++ citiesTurkey: " + citiesConcatenated)

        // 메소드로 ++ 할 수 있음
        citiesConcatenated = citiesEurope.++(citiesTurkey)
        println("citiesEurope.++(citiesTurky): " + citiesConcatenated)

        // 셋의 최대 및 최소 엘리먼트 찾음
        val evenNums = Set(2, 4, 6, 8)
        println("Minimum element in Set(2, 4, 6, 8): " + evenNums.min)
        println("Maximum element in Set(2, 4, 6, 8): " + evenNums.max)
    }
}