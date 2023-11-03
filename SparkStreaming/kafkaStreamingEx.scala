import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Properties

object kafkaStreamingEx {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().master("local[*]").appName("Streaming Ex").getOrCreate()
        val sc = spark.sparkContext

        val ssc = new StreamingContext(sc, Seconds(5))

        val kafkaReceiverParams = Map[String, Object](
            "bootstrap.servers" -> "localhost:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "whoami"
        )

        val topic = Array("orders")

        val kafkaStream = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](topic, kafkaReceiverParams)
        )

        val stream = kafkaStream.map(record => (record.key, record.value))

        case class Order(time: Timestamp, orderId: Long, clientId: Int,
                         symbol: String, amount: Int, price: Double, buy: Boolean)

        val orders = stream.flatMap(line => {
            val dateForamt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val s = line._2.split(",")
            try {
                assert(s(6) == "B" || s(6) == "S")
                List(Order(new Timestamp(dateForamt.parse(s(0)).getTime),
                    s(1).toLong, s(2).toInt, s(3), s(4).toInt, s(5).toDouble, s(6) == "B"))
            }
            catch {
                case e: Throwable => println("Wrong line format (" + e + "): " + line)
                    List()
            }
        })

        val numPerType = orders.map(x => (x.buy, 1L)).reduceByKey((c1, c2) => c1 + c2)
        val buySellList = numPerType.map(
            x => {
                if (x._1) ("BUYS", List(x._2.toString))
                else ("SELLS", List(x._2.toString))
            }
        )
        val amountPerClient = orders.map(x => (x.clientId, x.amount * x.price))

        val updateAmountState = (clientId: Int, amount: Option[Double], state: State[Double]) => {
                var total = amount.getOrElse(0.toDouble)
                if (state.exists()) total += state.get()

                state.update(total)
                Some((clientId, total))
        }

        val amountState = amountPerClient.mapWithState(
            StateSpec.function(updateAmountState)
        ).stateSnapshots()

        val top5cli = amountState.transform(
            _.sortBy(_._2, ascending = false).
            map(_._1).zipWithIndex().
            filter(x => x._2 < 5)
        )

        val top5cliList = top5cli.repartition(1).
            map(x => x._1.toString).
            glom().
            map(arr => ("TOP5CLIENTS", arr.toList))

        val stocksPerWindow = orders.map(x => (x.symbol, x.amount)).
            reduceByKeyAndWindow((a1: Int, a2: Int) => a1 + a2, Minutes(60))

        val topStocks = stocksPerWindow.transform(
            _.sortBy(_._2, ascending = false).map(_._1).
            zipWithIndex().filter(x => x._2 < 5)).repartition(1).
            map(x => x._1).glom().map(arr => ("TOP5STOCKS", arr.toList)
        )

        val finalStream = buySellList.union(top5cliList).union(topStocks)

        /*
        case class KafkaProducerWrapper(server: String) {
            val props = new Properties()
            props.put("bootstrap.servers", server)
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
            val producer = new KafkaProducer[String, String](props)

            def sendValues(topic: String, key: String, value: String): Unit = {
                producer.send(new ProducerRecord(topic, key, value))
            }
        }
        object KafkaProducerWrapper {
            var server = ""
            lazy val instance = new KafkaProducerWrapper(server)
        }

        이 방법 실패 ㅠㅠ..
         */


        // 이것도 실패.... 해결해보자..
        finalStream.foreachRDD((rdd) => {
            val props = new Properties()
            props.put("bootstrap.servers", "localhost:9092")
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
            rdd.foreachPartition((iter) => {
                val producer = new KafkaProducer[String, String](props)
                iter.foreach({
                    case (metric, list) => producer.send(
                        new ProducerRecord("metrics", metric, list.toString)
                    )
                })
                producer.close()
            })
        })

        val homeDir = System.getenv("$HOME")
        sc.setCheckpointDir(homeDir + "/checkpoint")
        ssc.start()
    }
}
