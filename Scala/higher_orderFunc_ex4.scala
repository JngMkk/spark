trait Curry {
    def curry[A, B, C](f: (A, B) => C): A => B => C
    def uncurry[A, B, C](f: A => B => C): (A, B) => C
}

object CurryImplement extends Curry {
    def uncurry[X, Y, Z](f: X => Y => Z): (X, Y) => Z = {
        (a: X, b: Y) => f(a)(b)
    }

    def curry[X, Y, Z](f: (X, Y) => Z): X => Y => Z = {
        (a: X) => {
            (b: Y) => f(a, b)
        }
    }
}

object CurryingHigherOrderFunc {
    def main(args: Array[String]): Unit = {
        def add(x: Int, y: Long): Double = x.toDouble + y

        val addSpicy = CurryImplement.curry(add)
        println(addSpicy(3)(1L))

        val increment = addSpicy(2)
        println(increment(1L))

        val unspicedAdd = CurryImplement.uncurry(addSpicy)
        println(unspicedAdd(1, 6L))
    }
}