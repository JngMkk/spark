import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.io.Source.fromFile

object SparkBasic {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val sc = spark.sparkContext

    // spark-submit 인수로 전달
    // val homeDir = System.getenv("HOME")
    // val inputPath = homeDir + "/workspace/sourceCode/github-archive/*.json"
    val ghLog = spark.read.json(args(0))
    val pushes = ghLog.filter("type = 'PushEvent'")

    // pushes.printSchema
    // println("all events: " + ghLog.count)
    // println("only pushes: " + pushes.count)
    // pushes.show(5)

    val grouped = pushes.groupBy("actor.login").count
    // grouped.show(5)

    val ordered = grouped.orderBy(grouped("count").desc)
    // ordered.show(5)
    // grouped.sort(col("count").desc).show(5)

    // 인수로 전달
    // val empPath = homeDir + "/workspace/sourceCode/first-edition/ch03/ghEmployees.txt"

    // 스칼라에서는 Set의 랜덤 룩업 성능이 Seq보다 빠름.
    // ++ 메서드는 빈 Set 객체에 복수 요소를 추가함
    val employees = Set() ++ (
      for {
        line <- fromFile(args(1)).getLines
      } yield line.trim
      )   // yield는 for 루프의 각 사이클별로 값(이 경우 line.trim) 하나를 임시 컬렉션에 추가함. 임시 컬렉션은 for 루프가 종료될 때 전체 for 표현식의 결과로 반환된 후 삭제됨.

    // 공유 변수 : 클러스터의 각 노드에 정확히 한 번만 전송.
    // 공유 변수는 클러스터 노드의 메모리에 자동으로 캐시되므로 프로그램 실행 중에 바로 접근할 수 있음.
    val bcEmployees = sc.broadcast(employees)

    // 사용자 정의 함수 udf 사용
    // 공유 변수에 접근하려면 반드시 value 메서드를 사용해야 함.
    val isEmp: (String) => (Boolean) = (arg: String) => bcEmployees.value.contains(arg)
    // 타입 추론 기능 활용
    // val isEmp2 = user => bcEmployees.value.contains(user)

    // val isEmployee = spark.udf.register("isEmpUDF", isEmp)
    // val isEmployee2 = spark.udf.register("isEmpUDF2", isEmp2)

    val isEmployee = udf(isEmp)
    // val isEmployee2 = udf(isEmp2)

    val filtered = ordered.filter(isEmployee($"login"))
    // val filtered2 = ordered.filter(isEmployee2($"login"))

    // filtered.show
    // filtered2.show()

    filtered.write.format(args(3)).save(args(2))
  }
}