// LinkedList
class GenericForLinkedList[X] {
    private class Node[X](elem: X) {
        var next: Node[X] = _
        override def toString = elem.toString
    }

    private var head: Node[X] = _

    def add(elem: X): Unit = {
        val value = new Node(elem)
        value.next = head
        head = value
    }

    private def printNodes(value: Node[X]): Unit = {
        if (value != null) {
            println(value)
            printNodes(value.next)
        }
    }

    def printAll(): Unit = {
        printNodes(head)
    }
}

object UsingGenericForLinkedList {
    def main(args: Array[String]): Unit = {
        val ints = new GenericForLinkedList[Int]()
        ints.add(1)
        ints.add(2)
        ints.add(3)
        ints.printAll()

        val strings = new GenericForLinkedList[String]()
        strings.add("Salman Khan")
        strings.add("Xamir Khan")
        strings.add("Shah Rukh Khan")
        strings.printAll()

        val doubles = new GenericForLinkedList[Double]()
        doubles.add(10.50)
        doubles.add(25.75)
        doubles.add(12.90)
        doubles.printAll()
    }
}