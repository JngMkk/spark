class ClassOne[T](val inp: T) {}

class ClassOneStr(one: ClassOne[String]) {
    def duplicatedString() = one.inp + one.inp
}

class ClassOneInt(one: ClassOne[Int]) {
    def duplicatedInt() = one.inp.toString + one.inp.toString
}

implicit def toStrMethods(one: ClassOne[String]) = new ClassOneStr(one)
implicit def toIntMethods(one: ClassOne[Int]) = new ClassOneInt(one)
/*
    컴파일러는 이제 ClassOne[String] 타입을 ClassOneStr 타입으로,
    ClassOne[Int] 타입을 ClassOneInt 타입으로 자동 변환함.
    따라서 두 클래스의 메서드를 ClassOne 객체에서도 호출할 수 있음.
*/

object implicitConversionEx {
    def main(args: Array[String]) = {
        val oneStrTest = new ClassOne("test")
        val oneIntTest = new ClassOne(123)

        oneStrTest.duplicatedString()
        oneIntTest.duplicatedInt()
    }
}