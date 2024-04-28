import org.apache.spark._

object BasicEx {
    def main(args: Array[String]) = {
        // LISENCE : 스파크가 사용하는 모든 라이브러리와 각 라이브러리가 제공한 라이선스 정보의 목록
        val licLines = sc.textFile("/home/jngmk/tools/spark/LICENSE")

        // licLines 컬렉션에 포함된 줄 개수 계산
        licLines.count

        // => : 스칼라의 함수 리터럴.
        val bsdLines = licLines.filter(x => x.contains("BSD"))
        bsdLines.count

        def isBSD(line: String) = {
            line.contains("BSD")
        }

        val isBSD2 = (line: String) => line.contains("BSD")

        val bsdLines2 = licLines.filter(isBSD)
        bsdLines2.count

        val bsdLines3 = licLines.filter(isBSD2)
        bsdLines3.count

        // 콘솔에 출력
        bsdLines.foreach(line => println(line))
        bsdLines.foreach(println)
    }
}