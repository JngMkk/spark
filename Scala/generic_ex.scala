// 제네릭 클래스는 타입을 파라미터로 사용하는 클래스.
// 제네릭 클래스는 컬렉션 클래스에 유용.
// 제네릭 클래스는 스택, 큐, 링크드 리스트 등과 같은 일상적인 데이터 구조 구현에 사용될 수 있음.

class GenericForStack[A] {
    private var elements: List[A] = Nil
    def push(x: A): Unit = {
        elements = x :: elements
    }
    def peek: A = elements.head
    def pop(): A = {
        val currentTop = peek
        elements = elements.tail
        currentTop
    }
}

object UsingGenericForStack {
    def main(args: Array[String]): Unit = {
        val stack = new GenericForStack[Int]
        stack.push(1)
        stack.push(2)
        stack.push(3)
        stack.push(4)
        println(stack.pop())
        println(stack.pop())
        println(stack.pop())
        println(stack.pop())
    }
}