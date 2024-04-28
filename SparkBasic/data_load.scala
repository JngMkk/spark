// 데이터 로드와 저장

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object DataLoadAndSaveEx {
    def main(args: Array[String]) = {
        val sc = new SparkContext(new SparkConf().setMaster("local[*]"))

        /*
            데이터 로드
                RDD에 데이터를 로드하는 작업은 SparkContext를 사용해 수행할 수 있음.
                    - textFile
                    - wholeTextFiles
                    - JDBC 데이터 소스에서 로드
                    ...
        */

        // textFile
        // 텍스트 파일을 RDD에 로드하기 위해 사용될 수 있으며 각 라인은 RDD의 엘리먼트가 됨
        // sc.textFile(name, minPartitions=None, use_unicode=True)
        val rdd1 = sc.textFile("wiki1.txt")         // org.apache.spark.rdd.RDD[String] = wiki1.txt MapPartitionsRDD[3] at textFile at <console>:23
        rdd1.count          // Long = 9


        // wholeTextFiles
        // 파일 이름과 파일의 전체 내용을 나타내는 <filename, textOfFile> 쌍을 포함하는 쌍으로 된 RDD에
        // 여러 텍스트 파일을 로드하기 위해 wholeTextFiles를 사용할 수 있음
        // 여러 개의 작은 텍스트 파일을 로드할 때 유용하며, textFile API와 다름
        // whileTextFiles가 사용될 때 파일의 전체 내용을 단일 레코드로 로드하기 때문
        // sc.wholeTextFiles(path, minPartitions=None, use_unicode=True)
        val rdd2 = sc.wholeTextFiles("wiki1.txt")   // org.apache.spark.rdd.RDD[(String, String)] = wiki1.txt MapPartitionsRDD[5] at wholeTextFiles at <console>:23
        rdd2.take(10)       // Array[(String, String)] = Array((file:/home/jngmk/wiki1.txt,"Apache Spark provides programmers with an application programming ..


        // JDBC 데이터 소스에서 로드
        // JDBC를 지원하는 외부 데이터 소스에서 데이터를 로드할 수 있음
        // sqlContext.load(path=None, source=None, schema=None, **options)
        val dbContent = sqlContext.load(source="jdbc", url="jdbc:mysql://localhost:3306/test",
                                        dbtable="test", partitionColumn="test")

        
        // RDD 저장
        // saveAsTextFile, saveAsObjectFile
        rdd2.saveAsTextFile("out.txt")
    }
}