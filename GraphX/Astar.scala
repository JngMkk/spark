import org.apache.spark.graphx._
import scala.reflect.ClassTag
import org.apache.spark.sql.SparkSession

object Astar extends Serializable {

    private val checkpointFrequency = 20
    /*
        Parameter
            - graph: A* 알고리즘을 실행할 그래프
            - origin: 시작 정점 ID
            - dest: 종료 정점 ID
            - maxIteration: 알고리즘의 최대 반복 횟수(기본값: 100)
            - estimateDistance: 두 정점의 속성 객체를 받고 둘 사이의 거리를 추정하는 함수
            - edgeWeight: 간선의 속성 객체를 받고 간선의 가중치를 반환하는 함수.
                          가중치는 해당 간선을 경로에 포함시키는 데 필요한 비용을 말함.
            - shouldVisitSource: 간선의 속성 객체를 인수로 받고 간선의 출발 정점을 방문할지 여부를 지정하는 함수.
                                 인수에 함수를 전달하지 않으면 모든 간선과 정점에 대해 true가 반환됨.
                                 이 함수를 사용하면 단방향 간선으로 구성된 그래프를 양방향 그래프처럼 다룰 수 있음.
            - shouldVisitDestination: 간선의 속성 객체를 인수로 받아 간선의 도착 정점을 방문할지 여부를 지정하는 함수.
                                      인수에 함수를 전달하지 않으면 모든 간선과 정점에 대해 true가 반환됨.
                                      이 함수를 사용하면 단방향 간선으로 구성된 그래프를 양방향 그래프처럼 다룰 수 있음.
            
            그래프가 이미 양방향 간선들로 구성된 경우 shouldVisitDestination 함수는 true를 반환해야 하고,
            shouldVisitSource 함수는 false를 반환해야 함.
    */
    def run[VD: ClassTag, ED: ClassTag](
        graph: Graph[VD, ED],
        origin: VertexId, dest: VertexId, maxIteration: Int = 100,
        estimateDistance: (VD, VD) => Double,
        edgeWeight: (ED) => Double,
        shouldVisitSource: (ED) => Boolean = (in: ED) => true,
        shouldVisitDestination: (ED) => Boolean = (in: ED) => true
    ): Array[VD] = {

        val resbuf = scala.collection.mutable.ArrayBuffer.empty[VD]

        // 인수로 전달된 시작 정점 ID와 종료 정점 ID가 그래프에 존재하는지 확인
        val arr = graph.vertices.flatMap(
            x => if (x._1 == origin || x._1 == dest) List[(VertexId, VD)](x) else List()
        ).collect()
        
        if (arr.length != 2) {
            throw new IllegalArgumentException("Origin or Destination not found")
        }
        
        // 첫 번째 배열의 첫 번째 값이 시작 정점 ID와 같으면 시작 정점 VD를 선택
        val originNode = if (arr(0)._1 == origin) arr(0)._2 else arr(1)._2
        val destNode = if (arr(0)._1 == origin) arr(1)._2 else arr(0)._2

        // 시작 정점과 종료 정점 사이의 거리를 추정함
        var dist = estimateDistance(originNode, destNode)

        // F, G, H 값을 계산하는 데 사용할 작업그래프
        case class WorkNode(
            // 원본 그래프의 노드 객체를 originNode 필드에 저장
            originNode: VD,
            g: Double = Double.MaxValue,
            h: Double = Double.MaxValue,
            f: Double = Double.MaxValue,
            visited: Boolean = false,
            // 최종 경로를 구성하는 데 사용할 이전 정점 ID
            predec: Option[VertexId] = None
        )

        // 원본 그래프의 정점을 WorkNode 객체로 매핑해 작업그래프를 생성
        var gwork = graph.mapVertices {
            case(ind, node) => {
                // 시작 정점
                if (ind == origin) WorkNode(node, 0, dist, dist)
                // 나머지 정점
                else WorkNode(node)
            }
        }.cache()   // 그래프를 재사용하기위한 캐싱

        /*
            그래프의 캐싱과 체크포인팅
                그래프 알고리즘은 대부분 동일한 데이터를 반복적으로 재사용함.
                그러므로 그래프 데이터를 메모리에서 바로 사용할 수 있도록 준비하는 것이 좋음.
                스파크에서는 Graph의 cache 메서드를 사용해 그래프의 정점과 간선을 캐싱할 수 있음.
                기본적으로 정점과 간선 데이터를 메모리에만 캐싱하지만,
                persist 메서드로 메모리 이외의 다른 스토리지 레벨을 지정할 수도 있음.
                persist 메서드는 StorageLevel 객체를 인수로 전달해 호출하며,
                StorageLevel 객체에는 DISK_ONLY, MEMORY_AND_DISK 등 다양한 스토리지 레벨이 상수로 정의되어 있음.

                그러나 그래프의 데이터를 반복적으로 변경하고 캐싱하면 가용 메모리 공간을 빠르게 채우고 JVM이 가비지 컬렉션을 자주 수행함.
                스파크는 JVM보다 더 효율적으로 캐시 데이터를 해제할 수 있으므로
                그래프 데이터를 자주 변경하고 메모리에 유지하려면 그래프를 적절한 시점에 제거해야 함.
                그래프를 메모리에서 제거할 때는 간선과 정점을 모두 제거하는 unpersist 메서드를 사용하거나
                unpersistVertices 메서드로 정점만 제거할 수 있음.
                두 메서드 모두 데이터를 캐시에서 완전히 제거할 때까지 프로그램이 기다릴지 여부를 지정하는 Boolean 값을 인수로 받음.
        */

        // 시작 정점을 현재 정점으로 설정
        var currVertexId: Option[VertexId] = Some(origin)

        var lastIter = 0

        /*
            1. 현재 정점을 방문 완료로 변경
            2. 현재 정점과 인접한 이웃 정점들의 F, G, H 값을 계산
            3. 미방문 그룹에서 다음 반복 차수의 현재 정점을 선정
        */
        for (
        iter <- 0 to maxIteration
        // currVertexId가 nonEmpty가 될때 까지
        if currVertexId.isDefined;
        // currVertextId가 종료 정점에 도착할때 까지 반복
        if currVertexId.getOrElse(Long.MaxValue) != dest) 
        {
            // 반복 횟수
            lastIter = iter
            println("Iteration " + iter)
            
            // 그래프 정점을 캐시에서 제거함. 간선은 수정하지 않으므로 캐시에 그대로 남겨 둠.
            gwork.unpersistVertices()

            // 정점 RDD 변환 후 캐싱
            // 현재 정점을 방문 완료(true)로 변경
            gwork = gwork.mapVertices((id: VertexId, v: WorkNode) => {
                if (id != currVertexId.get) v
                else WorkNode(v.originNode, v.g, v.h, v.f, true, v.predec)
            }).cache()
                
            /*
                체크포인팅을 사용하지 않으면 루프를 반복할수록 그래프 RDD의 DAG가 계속 늘어나면서 재계산에 필요한 연산량이 점차 증가함.
                체크포인팅 없이 루프를 너무 많이 반복하면 작업그래프가 계속 변환되면서 스택 오버플로 오류가 발생할 수 있음.
                체크포인팅은 DAG 계획을 저장해 스택 오버플로 오류를 방지함.
            */
            if (iter % checkpointFrequency == 0) gwork.checkpoint()

            // 현재 정점의 이웃 정점에서 F, G, H 값 계산.
            // neighbors: 현재 정점과 인접 정점만 필터링
            val neighbors = gwork.subgraph(
                trip => trip.srcId == currVertexId.get || trip.dstId == currVertexId.get
            )

            /*
                서브그래프 정점 중에서 기방문 그룹에 포함되지 않으면서
                shouldVisitSource 또는 shouldVisitDestination 메서드가 true를 반환하는 정점에 메시지 전송.
                메서드 인수로 제공된 edgeWeight 함수의 호출 결과를 현재 정점 G 값에 더해 새로운 G 값을 계산하고,
                EdgeContext의 sendToSrc 또는 sendToDst 메서드를 사용해 이 값을 이웃 정점으로 전송함.
                새 G값을 전달받은 정점들로만 구성된 newGs VertexRDD를 반환함.
            */
            val newGs = neighbors.aggregateMessages[Double](
                ctx => {
                    if (ctx.srcId == currVertexId.get && !ctx.dstAttr.visited && shouldVisitDestination(ctx.attr)) {
                        ctx.sendToDst(ctx.srcAttr.g + edgeWeight(ctx.attr))
                    } else if (ctx.dstId == currVertexId.get && !ctx.srcAttr.visited && shouldVisitSource(ctx.attr)) {
                        ctx.sendToSrc(ctx.dstAttr.g + edgeWeight(ctx.attr))
                    }
                },
                (a1: Double, a2: Double) => a1,
                TripletFields.All
            )

            /*
                newGs를 작업그래프와 조인함.
                각 이웃 정점의 새로운 F 값이 기존 F 값보다 작을 때,
                알고리즘은 predec 필드(이전 정점)에 현재 정점 ID를 설정하고 정점의 속성 객체를 바꿈.
                앞서 새 G 값을 할당받지 못한 정점은 newGs에 포함되지 않음.
                이때 totalG 변수에 None이 전달되며, 조인 함수는 기존 정점 객체를 그대로 유지함.
            */
            val cid = currVertexId.get

            gwork = gwork.outerJoinVertices(newGs)(
                (_, node, totalG) => {
                    totalG match {
                        case None => node
                        case Some(newG) => {
                            if (node.h == Double.MaxValue) {
                                val h = estimateDistance(node.originNode, destNode)
                                WorkNode(node.originNode, newG, h, newG + h, false, Some(cid))
                            } else if (node.h + newG < node.f) {
                                WorkNode(node.originNode, newG, node.h, newG + node.h, false, Some(cid))
                            } else {
                                node
                            }
                        }
                    }
                }
            )

            // 미방문 그룹에 속한 정점들을 가져옴
            val openList = gwork.vertices.filter(v => v._2.h < Double.MaxValue && !v._2.visited)

            // 미방문 정점이 남았다면 가장 작은 F 값을 가진 정점의 ID를 찾아서 currVertexId에 저장.
            if (openList.isEmpty) currVertexId = None
            else {
                val nextV = openList.map(v => (v._1, v._2.f)).reduce((n1, n2) => if (n1._2 < n2._2) n1 else n2)
                currVertexId = Some(nextV._1)
            }
        }

        // 종료 정점에 도착했다면 종료 정점에서 시작해 각 정점의 predec 필드를 따라
        // 시작 정점까지 되돌아가면서 경로상의 각 정점 객체를 ArrayBuffer(resbuf 변수)에 추가함.
        if (currVertexId.isDefined && currVertexId.get == dest) {
            var currId: Option[VertexId] = Some(dest)
            var it = lastIter
            while (currId.isDefined && it >= 0) {
                val v = gwork.vertices.filter(x => x._1 == currId.get).collect()(0)
                resbuf += v._2.originNode
                currId = v._2.predec
                it -= 1
            }
        } else println("Path not found")

        gwork.unpersist()
        resbuf.toArray.reverse
    }

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().getOrCreate()
        val sc = spark.sparkContext

        // 알고리즘 테스트를 위한 3차원 점을 표현할 클래스
        case class Point(x: Double, y: Double, z: Double)

        val vertices3d = sc.parallelize(Array(
            (1L, Point(1, 2, 4)), (2L, Point(6, 4, 4)),
            (3L, Point(8, 5, 1)), (4L, Point(2, 2, 2)),
            (5L, Point(2, 5, 8)), (6L, Point(3, 7, 4)),
            (7L, Point(7, 9, 1)), (8L, Point(7, 1, 2)),
            (9L, Point(8, 8, 10)), (10L, Point(10, 10, 2)),
            (11L, Point(8, 4, 3))
        ))

        val edges3d = sc.parallelize(Array(
            Edge(1, 2, 1.0), Edge(2, 3, 1.0),
            Edge(3, 4, 1.0), Edge(4, 1, 1.0),
            Edge(1, 5, 1.0), Edge(4, 5, 1.0),
            Edge(2, 8, 1.0), Edge(4, 6, 1.0),
            Edge(5, 6, 1.0), Edge(6, 7, 1.0),
            Edge(7, 2, 1.0), Edge(2, 9, 1.0),
            Edge(7, 8, 1.0), Edge(7, 10, 1.0),
            Edge(10, 11, 1.0), Edge(9, 11, 1.0)
        ))

        val graph3d = Graph(vertices3d, edges3d)

        // 3차원 공간 내 두 점 사이 거리
        val calcDistance3d = (p1: Point, p2: Point) => {
            val x = p1.x - p2.x
            val y = p1.y - p2.y
            val z = p1.z - p2.z
            Math.sqrt(x*x + y*y + z*z)
        }

        val graph3dDst = graph3d.mapTriplets(t => calcDistance3d(t.srcAttr, t.dstAttr))

        val homeDir = System.getenv("HOME")
        sc.setCheckpointDir(homeDir + "/checkpoint")

        Astar.run(graph3dDst, 1, 10, 50, calcDistance3d, (e: Double) => e)
    }
}