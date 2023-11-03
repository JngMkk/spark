import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.hadoop.io.Text

object NewAPIHadoopFileEx {
    def main(args: Array[String]) = {
        val sc = new SparkContext(new SparkConf().setMaster("local[*]"))

        val newHadoopRdd = sc.newAPIHadoopFile("statesPopulation.csv",
                                                classOf[KeyValueTextInputFormat],
                                                classOf[Text],
                                                classOf[Text])
        // org.apache.spark.rdd.RDD[(org.apache.hadoop.io.Text, org.apache.hadoop.io.Text)] = statesPopulation.csv NewHadoopRDD[26] at newAPIHadoopFile at <console>:25

        newHadoopRdd.count
        // Long = 351
    }
}