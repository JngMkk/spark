// 케이스 클래스는 자동으로 생성되는 메소드를 포함한 인스턴스를 생성할 수 있는 클래스.
// 케이스 클래스는 자동으로 생성되는 컴패니언 오브젝트도 포함함.
// case class <식별자> ([var] <식별자>: <타입>[, ...]) [extends <식별자>(<입력 파라미터>)] [{ 필드와 메소드 }]
// 케이스 클래스는 패턴 매칭이 가능하고
// hashCode(위치 / 범위는 클래스)
// apply(위치 / 범위는 오브젝트)
// copy(위치 / 범위는 클래스)
// equals(위치 / 범위는 클래스)
// toString(위치 / 범위는 클래스)
// unapply(위치 / 범위는 오브젝트)
// 위의 메소드가 내부에 이미 구현돼 있음

object CaseClass {
    def main(args: Array[String]): Unit = {
        case class Character(name: String, isHacker: Boolean)

        val nail = Character("Nail", true)

        val joyce = nail.copy(name = "Joyce")

        println(nail == joyce)

        println(nail.equals(joyce))

        println(nail.equals(nail))

        println(nail.hashCode())

        println(nail)

        joyce match {
            case Character(x, true) => s"$x is a hacker"
            case Character(x, false) => s"$x is not a hacker"
        }
    }
}