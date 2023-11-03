// try {
//     // 코드 작성
// } catch {
//     case foo: FooException => handleFooException(foo)
//     case bar: BarException => handleBarException(bar)
//     case _: Throwable => println("Got some other kind of exception")
// } finally {
//     // 데이터베이스 커넥션을 종료하는 것과 같은 코드는 여기 작성
// }

import java.io.IOException
import java.io.FileReader
import java.io.FileNotFoundException

object TryCatch {
    def main(args: Array[String]): Unit = {
        try {
            val f = new FileReader("data/data.txt")
        } catch {
            case ex: FileNotFoundException => println("File not found exception")
            case ex: IOException => println("IO Exception")
        } finally {
            // 발생한 예외와 상관없이 코드를 실행
            println("Hi!")
        }
    }
}