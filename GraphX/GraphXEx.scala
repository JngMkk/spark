import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators

object GraphXEx {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().getOrCreate()
        val sc = spark.sparkContext

        case class User(name: String, occupation: String)

        val users = sc.textFile("users.txt").map({
            line =>
                val fields = line.split(",")
                (fields(0).toLong, User(fields(1), fields(2)))
        })
        users.take(10)

        val friends = sc.textFile("friends.txt").map({
            line =>
                val fields = line.split(",")
                Edge(fields(0).toLong, fields(1).toLong, "friend")
        })
        friends.take(10)

        // 모든 정보를 모아 버텍스와 에지 목록에서 Graph 생성
        val graph = Graph(users, friends)
        graph.vertices
        // org.apache.spark.graphx.VertexRDD[User] = VertexRDDImpl[15] at RDD at VertexRDD
        graph.edges
        // org.apache.spark.graphx.EdgeRDD[String] = EdgeRDDImpl[17] at RDD at EdgeRDD
        graph.vertices.collect
        graph.edges.collect

        // filter
        graph.vertices.filter(x => x._1 == 2).collect
        graph.vertices.filter(x => x._2.name == "Mark").collect
        graph.vertices.filter(x => x._2.occupation == "Doctor").collect

        // 소스 vertexID나 대상 vertexID를 사용해 에지에서도 필터를 수행할 수 있음.
        graph.edges.filter(x => x.srcId == 1).collect

        // mapValues
        graph.vertices.mapValues((_, u) => u.name).collect
        graph.edges.mapValues(x => s"${x.srcId} -> ${x.dstId}").take(5)

        // aggregateMessages
        // 모든 대상 버텍스에 메시지를 보냄. 각 버텍스에서 병합 전략은 수신되는 모든 메시지를 추가하는 것.
        graph.aggregateMessages[Int](_.sendToDst(1), _ + _).collect
        // Array((4,2), (6,2), (8,3), (10,4), (2,3), (1,3), (3,4), (7,3), (9,2), (5,2))

        // triangleCount
        // 삼각형을 버텍스와 결합해 각 사용자의 출력과 사용자가 속한 삼각형을 생성해 그래프의 삼각형을 계산하는 데 사용되는 코드
        val triangleCounts = graph.triangleCount.vertices
        // org.apache.spark.graphx.VertexRDD[Int] = VertexRDDImpl[91] at RDD at VertexRDD
        triangleCounts.take(10)
        // Array[(org.apache.spark.graphx.VertexId, Int)] = Array((4,0), (6,1), (8,1), (10,1), (2,2), (1,1), (3,2), (7,1), (9,0), (5,0))

        val triangleCountsPerUser = users.join(triangleCounts).map({
            case (_, (User(x, y), k)) => ((x, y), k)
        })
        triangleCountsPerUser.collect.mkString("\n")
        /*
        ((Liz,Doctor),0)
        ((Beth,Accountant),1)
        ((Mary,Cashier),1)
        ((Ken,Librarian),1)
        ((Mark,Doctor),2)
        ((John,Accountant),1)
        ((Sam,Lawyer),2)
        ((Larry,Engineer),1)
        ((Dan,Doctor),0)
        ((Eric,Accountant),0)
         */

        // Pregel API
        // connected component
        // connectedComponents
        // 단 하나의 연결된 컴포넌트를 갖고 있기 때문에 모든 사용자의 컴포넌트 번호를 표시함.
        graph.connectedComponents.vertices.collect
        // Array[(org.apache.spark.graphx.VertexId, org.apache.spark.graphx.VertexId)] = Array((4,1), (6,1), (8,1), (10,1), (2,1), (1,1), (3,1), (7,1), (9,1), (5,1))

        graph.connectedComponents.vertices.join(users).take(10)

        // 최단 경로
        lib.ShortestPaths.run(graph, Seq(1)).vertices.join(users).collect.mkString("\n")
        /*
        String =
        (4,(Map(1 -> 2),User(Liz,Doctor)))
        (6,(Map(1 -> 2),User(Beth,Accountant)))
        (8,(Map(1 -> 2),User(Mary,Cashier)))
        (10,(Map(1 -> 1),User(Ken,Librarian)))
        (2,(Map(1 -> 1),User(Mark,Doctor)))
        (1,(Map(1 -> 0),User(John,Accountant)))
        (3,(Map(1 -> 1),User(Sam,Lawyer)))
        (7,(Map(1 -> 2),User(Larry,Engineer)))
        (9,(Map(1 -> 3),User(Dan,Doctor)))
        (5,(Map(1 -> 2),User(Eric,Accountant)))
         */

        // edge 가중치 사용 예시
        val graphEx = GraphGenerators.logNormalGraph(sc, numVertices = 10).mapEdges(e => e.attr.toDouble)
        val srcId = 0
        val initGraphEx = graphEx.mapVertices(
            (id, _) =>
                if (id == srcId) 0.0
                else Double.PositiveInfinity
        )

        val weightedShortestPathEx = initGraphEx.pregel(Double.PositiveInfinity)(
            (_, dist, newDist) => math.min(dist, newDist),
            triplet => {
                if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
                    Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
                } else {
                    Iterator.empty
                }
            },
            (a, b) => math.min(a, b)
        )

        weightedShortestPathEx.vertices.take(10).mkString("\n")
        /*
        String =
        (0,0.0)
        (6,2.0)
        (1,2.0)
        (7,2.0)
        (8,1.0)
        (2,1.0)
        (3,1.0)
        (9,2.0)
        (4,1.0)
        (5,1.0)
         */


        // 예제 데이터에 적용
        val distance = sc.textFile("friends.txt").map({
            line =>
                val fields = line.split(",")
                Edge(fields(0).toLong, fields(1).toLong, fields(2).toDouble)
        })

        val newGraph = Graph(users, distance)
        val srcId3 = 1
        val initGraph = newGraph.mapVertices(
            (id, _) =>
                if (id == srcId3) 0.0
                else Double.PositiveInfinity
        )
        val weightedShortestPath = initGraph.pregel(Double.PositiveInfinity, 5)(
            (_, dist, newDist) => math.min(dist, newDist),
            triplet => {
                if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
                    Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
                } else {
                    Iterator.empty
                }
            },
            (a, b) => math.min(a, b)
        )
        weightedShortestPath.vertices.take(10).mkString("\n")
        /*
        (4,8.0)
        (6,6.0)
        (8,6.0)
        (10,5.0)
        (2,1.0)
        (1,0.0)
        (3,3.0)
        (7,5.0)
        (9,5.0)
        (5,4.0)
         */


        // PageRank
        val prVertices = graph.pageRank(0.0001).vertices
        prVertices.join(users).sortBy(_._2._1, ascending = false).take(10).mkString("\n")
        /*
        String =
        (10,(1.3894313412237382,User(Ken,Librarian)))
        (3,(1.3539230771280086,User(Sam,Lawyer)))
        (8,(1.1039233153727108,User(Mary,Cashier)))
        (7,(1.0422534052764745,User(Larry,Engineer)))
        (2,(1.0228352257412907,User(Mark,Doctor)))
        (1,(1.0228080376165396,User(John,Accountant)))
        (9,(0.791862358371651,User(Dan,Doctor)))
        (5,(0.7742753377329513,User(Eric,Accountant)))
        (6,(0.7580953358596937,User(Beth,Accountant)))
        (4,(0.7405925656769419,User(Liz,Doctor)))
         */
    }
}