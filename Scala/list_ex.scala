object ListEx {
    def main(args: Array[String]) = {
        // List는 불변 컬렉션
        val numbers = List(1, 2, 3, 4, 5, 1, 2, 3, 4, 5)
        println(numbers)
        // numbers(3) = 10 -> 에러

        // 이렇게 List 정의 가능
        val numbers2 = 1 :: 2 :: 3 :: 4 :: 5 :: 1 :: 2 :: 3 :: 4 :: 5 :: Nil
        println(numbers2)

        val cities = "Dublin" :: "London" :: "NY" :: Nil
        val nums = 2 :: 4 :: 6 :: 8 :: Nil
        val empty = Nil
        
        // 2차원 리스트
        val dim = 1 :: 2 :: 3 :: Nil ::
                        4 :: 5 :: 6 :: Nil ::
                            7 :: 8 :: 9 :: Nil :: Nil
        val temp = Nil

        println("Head of cities: " + cities.head)
        println("Tail of cities: " + cities.tail)

        println("Check if cities is empty: " + cities.isEmpty)
        println("Check if temp is empty: " + temp.isEmpty)

        val citiesEurope = "Dublin" :: "London" :: "Berlin" :: Nil
        val citiesTurkey = "Istanbul" :: "Ankara" :: Nil

        // :::를 이용해 2개 이상의 리스트를 합침
        var citiesConcatenated = citiesEurope ::: citiesTurkey
        println("citiesEurope ::: citiesTurkey: " + citiesConcatenated)

        // concat 사용
        citiesConcatenated = List.concat(citiesEurope, citiesTurkey)
        println("List.concat(citiesEurope, citiesTurkey): " + citiesConcatenated)
    }
}