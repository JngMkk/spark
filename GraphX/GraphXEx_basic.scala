object GraphXEx_basic {
    def main(args: Array[String]): Unit = {
        case class Person(name: String) {
            val friends = scala.collection.mutable.ArrayBuffer[Person]()

            def numOfFriends() = friends.length

            def isFriend(other: Person) = friends.find(_.name == other.name)

            def isConnectedWithin2Steps(other: Person) = {
                for {f <- friends} yield {
                    f.name == other.name || f.isFriend(other).isDefined
                }
            }.contains(true)
        }

        val john = Person("John")
        val ken = Person("Ken")
        val mary = Person("Mary")
        val dan= Person("Dan")

        println(john.numOfFriends())
        john.friends += ken
        println(john.numOfFriends())

        ken.friends += mary
        println(ken.numOfFriends())

        mary.friends += dan
        println(mary.numOfFriends())

        println(john.isFriend(ken))
        println(john.isFriend(mary))
        println(john.isFriend(dan))

        println(john.isConnectedWithin2Steps(ken))
        println(john.isConnectedWithin2Steps(mary))
        println(john.isConnectedWithin2Steps(dan))
    }
}