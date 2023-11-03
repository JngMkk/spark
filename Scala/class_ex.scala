class Hello(primaryMessage: String, secondaryMessage: String) {
    def this(primaryMessage: String) = this(primaryMessage, "")
    def sayHello() = println(primaryMessage + secondaryMessage)
}
object Constructors {
    def main(args: Array[String]): Unit = {
        val hello = new Hello("Hello World!", "I'm in a trouble, please help me out.")
        hello.sayHello()
    }
}