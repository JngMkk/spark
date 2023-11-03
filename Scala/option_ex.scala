// Option
// 스칼라 Option[T]는 주어진 타입에 대해 0개 이상의 엘리먼트를 담는 컨테이너.
// Option[T]는 Some[T] 또는 누락된 값을 나타내는 None 객체가 될 수 있음.
// 예를 들어 스칼라 Map의 get 메소드는 Map에 주어진 키에 해당하는 값이 있으면 Some(value)를 생성하고
// Map에 해당 키가 정의돼 있지 않으면 None을 생성

object Option {
    def show(x: Option[String]) = {
        x match {
            case Some(s) => s
            case None => "?"
        }
    }

    def main(args: Array[String]) = {
        val megacity = Map("Bangladesh" -> "Dhaka", "Japan" -> "Tokyo", "India" -> "Kolkata", "USA" -> "New York")

        println("megacity.get(\"Bangladesh\"): " + show(megacity.get("Bangladesh")))
        println("megacity.get(\"India\"): " + show(megacity.get("India")))
        println("megacity.get(\"Korea\"): " + show(megacity.get("Korea")))

        // getOrElse() 메소드를 사용할 때 값이 있으면 해당 값에 접근하고
        // 값이 없으면 기본 값에 접근할 수 있음
        // getOrElse[B >: A](default: => B): B
        // 옵션의 값이 비어 있지 않으면 옵션의 값을 리턴함.
        val message: Option[String] = Some("Hello, world!")
        val x: Option[Int] = Some(20)
        val y: Option[Int] = None

        println("message.getOrElse(0): " + message.getOrElse(0))
        println("x.getOrElse(0): " + x.getOrElse(0))
        println("y.getOrElse(10): " + y.getOrElse(10))

        // isEmpty 메소드를 사용하면 옵션이 None인지 여부를 확인할 수 있음
        println("message.isEmpty: " + message.isEmpty)
        println("x.isEmpty: " + x.isEmpty)
        println("y.isEmpty: " + y.isEmpty)

        // Map.get 메소드는 사용자가 접근하려고 시도하는 엘리먼트가 존재하는지 여부를 알기 위해 Option을 사용함
        val numbers = Map("two" -> 2, "four" -> 4)
        println(numbers.get("four"))    // Some(4)
        println(numbers.get("five"))    // None


        // exists
        // 조건을 적용할 때 Traversable 컬렉션에서 적어도 하나 이상의 엘리먼트를 포함하는지 확인
        // def exists(p: ((A, B)) => Boolean): Boolean
        val cityList = List("Dublin", "NY", "Cairo")
        val ifExistsinList = cityList.exists(x => x == "Dublin")
        println(ifExistsinList)
        // true

        val cityMap = Map("Ireland" -> "Dublin", "UK" -> "London")
        val ifExistsinMap = cityMap.exists(x => x._2 == "Dublin")
        println(ifExistsinMap)
        // true


        // forall
        // Traversable 컬렉션의 모든 엘리먼트가 특정 조건에 부합하는지 확인.
        // forall (p: (A) => Boolean): Boolean
        println(Vector(1, 2, 8, 10).forall(x => x % 2 == 0))
        // false


        // filter
        // 특정 조건을 충족시키는 모든 엘리먼트를 선택
        // filter(p: (A) => Boolean): Traversable[A]
        println(List(("Dublin", 2), ("NY", 8), ("London", 8)).filter(x => x._2 >= 5))
        // List((NY, 8), (London, 8))


        // map
        // 특정 함수를 컬렉션의 모든 엘리먼트를 순회하게 한 후
        // 새로운 컬렉션이나 엘리먼트 집합을 생성하는 데 사용
        // map[B](f: (A) => B): Map[B]
        // 해당 리스트의 모든 값의 제곱을 계산한 리스트를 얻음
        println(List(2, 4, 5, -6).map(x => x * x))
        // List[Int] = List(4, 16, 25, 36)


        // take
        // 컬렉션의 처음 n번 째 엘리먼트를 가져 오기 위해 사용
        // take(n: Int): Traversable[A]
        def odd: Stream[Int] = {
            def odd0(x: Int): Stream[Int] = {
                if (x % 2 != 0) x #:: odd0(x + 1)
                else
                    odd0(x + 1)
            }
            odd0(1)
        }
        // 처음 5번째 홀수에 대한 리스트를 얻음
        println((odd take(5)).toList)
        // List(1, 3, 5, 7, 9)


        // groupBy
        // 특정 파티셔닝 함수를 사용해 특정 컬렉션을
        // 서로 다른 Traversable 컬렉션의 맵으로 분할하는 데 사용
        // groupBy[K](f: ((A, B)) => K): Map[K, Map[A, B]]
        // 해당 리스트에서 양수와 음수를 그룹핑
        println(List(1, -2, 3, -4).groupBy(x => if (x >= 0) "positive" else "negative"))
        // HashMap(negative -> List(-2, -4), positive -> List(1, 3))


        // init
        // Traversable 컬렉션에서 마지막 엘리먼트를 제외한 나머지 엘리먼트를 선택함
        // init: Traversable[A]
        println(List(1, 2, 3, 4).init)
        // List(1, 2, 3)


        // drop
        // 첫 번째 n개의 엘리먼트를 제외한 모든 엘리먼트를 선택하기 위해 사용
        // drop(n: Int): Traversable[A]
        println(List(1, 2, 3, 4).drop(3))
        // List(4)


        // takeWhile
        // 조건이 만족될 때까지 엘리먼트 집합을 얻기 위해 사용
        // takeWhile(p: (A) => Boolean): Traversable[A]
        // 리스트의 엘리먼트가 9를 넘지 않을 때까지 모든 홀수 엘리먼트의 리스트를 리턴
        println(odd.takeWhile(x => x < 9).toList)
        // List(1, 3, 5, 7)


        // dropWhile
        // 조건을 만족할 때까지 엘리먼트 집합을 얻고 싶지 않을 때 사용
        // dropWhile(p: (A) => Boolean): Traversable[A]
        println(List(2, 3, 4, 9, 10, 11).dropWhile(x => x < 5))
        // List(9, 10, 11)

        
        // flatMap
        // 함수를 중첩 리스트의 파라미터로 사용하고 함수의 출력을 다시 결합하는 사용자 정의 함수를 사용하고 싶을 때
        // flatMap[B](f: (A) => GenTraversableOnce[B]): Traversable[B]
        // 중첩 리스트에 함수를 적용하고 함수 결과를 다시 결합
        println(List(List(2, 4), List(6, 8)).flatMap(x => x.map(x => x * x)))
        // List(4, 16, 36, 64)

        
        // fold, reduce, aggregate, collect, count, find, zip과 같은 메소드는
        // 하나의 컬렉션에서 다른 컬렉션으로 전달하기 위해 사용될 수 있음(toVector, toSeq, toSet, toArray)
    }
}