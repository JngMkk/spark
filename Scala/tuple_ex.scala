// 스칼라 튜플은 고정된 개수의 엘리먼트를 함께 결합하기 위해 사용됨.
// 튜플의 궁극적인 목표는 익명 함수를 도와 익명 함수에 튜플을 전부 전달할 수 있게 하는 것
// 튜플은 각 엘리먼트의 타입 정보를 유지하면서 다른 타입의 객체를 가질 수 있음 (배열, 리스트와 차이점)
// 스칼라 관점에서 스칼라 튜플은 불변.
object TupleEx {
    def main(args: Array[String]) = {
        var t1 = (20, "Hello", Console)
        t1.productIterator.foreach {
            t1 => println("Value = " + t1)
        }

        val t2 = new Tuple3(20, "Hello", Console)
        t2.productIterator.foreach {
            t2 => println("Value = " + t2)
        }

        val cityPop = ("Dublin", 2)

        // 튜플 데이터에 접근하려면 이름 접근자 대신 위치를 기반으로 접근할 수 있음.
        // 위치를 기반으로 접근할 때 위치 접근자는 0이 아닌 1 기반.
        println(cityPop._1)
        println(cityPop._2)

        // 패턴 매칭에 완벽히 어울림.
        // cityPop match {
        //     case ("Dublin", population) => ...
        //     case ("NY", population) => ...
        // }

        // 2개의 값으로 구성된 튜플에 대한 간단한 구문을 작성하기 위해 특수 연산자인 ->를 사용할 수도 있음.
        println("Dublin" -> 2)

        val evenTuple = (2, 4, 6, 8)
        val sumTupleElems = evenTuple._1 + evenTuple._2 + evenTuple._3 + evenTuple._4
        println("Sum of Tuple Elements: " + sumTupleElems)

        // foreach 메소드를 사용해 튜플을 순회하고 튜플의 엘리먼트를 출력할 수 있음
        evenTuple.productIterator.foreach {
            evenTuple => println("Value = " + evenTuple)
        }
    }
}