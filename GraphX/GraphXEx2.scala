import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._

object GraphXEx2 {
    case class Person(name: String, age: Int)
    case class Relationship(relation: String)
    case class PersonExt(name: String, age: Int, children: Int = 0, friends: Int = 0, married: Boolean = false)
    def main(args: Array[String]): Unit ={
        val spark = SparkSession.builder().getOrCreate()
        val sc = spark.sparkContext

        val vertices = sc.parallelize(
            Array((1L, Person("Homer", 39)), (2L, Person("Marge", 39)),
                (3L, Person("Bart", 12)), (4L, Person("Milhouse", 12)))
        )

        val edges = sc.parallelize(
            Array(Edge(4L, 3L, "friend"), Edge(3L, 1L, "father"),
                Edge(3L, 2L, "mother"), Edge(1L, 2L, "marriedTo"))
        )

        val graph = Graph(vertices, edges)
        graph.vertices.count
        graph.edges.count

        val newgraph = graph.mapEdges(
            (_, iter) =>
                iter.map(edge => Relationship(edge.attr))
        )
        newgraph.edges.collect()

        val newGraphExt = newgraph.mapVertices(
            (_, person) => PersonExt(person.name, person.age)
        )

        val aggVertices = newGraphExt.aggregateMessages(
            (ctx: EdgeContext[PersonExt, Relationship, (Int, Int, Boolean)]) => {
                if (ctx.attr.relation == "marriedTo") {
                    ctx.sendToSrc((0, 0, true)); ctx.sendToDst((0, 0, true))
                } else if (ctx.attr.relation == "mother" || ctx.attr.relation == "father") {
                    ctx.sendToDst((1, 0, false))
                } else if (ctx.attr.relation.contains("friend")) {
                    ctx.sendToDst((0, 1, false)); ctx.sendToSrc((0, 1, false))
                }
            },
            (msg1: (Int, Int, Boolean), msg2: (Int, Int, Boolean)) =>
                (msg1._1 + msg2._1, msg1._2 + msg2._2, msg1._3 || msg2._3)
        )
        // vertexRDD
        aggVertices.collect.foreach(println)

        /*
        outerJoinVertices

            def outerJoinVertices[U: ClassTag, VD2: ClassTag](
                other: RDD[(VertexId, U)])(mapFunc: (VertexId, VD, Option[U]) => VD2): Graph[VD2, ED]

            첫 번째 인수에는 (정점 ID, 정점 속성 객체) 쌍의 튜플로 구성된 RDD를 전달하고,
            두 번째 인수에는 기존 그래프의 정점 속성 객체(임의의 타입 VD)와
            새로 입력한 RDD의 정점 속성 객체(임의의 타입 U)를 결합하는 매핑 함수를 전달함.
            기존 그래프의 정점 중 새로 입력한 RDD에 포함되지 않은 정점의 경우,
            매핑 함수의 세 번째 인수인 Option[U]에는 None 객체가 전달됨.

            매핑 함수는 각 정점에 전달된 메시지가 있을 때(즉, 각 정점의 새 속성 객체가 존재할 때)
            메시지에 담긴 합산 결과와 기존 name 및 age 속성을 새로운 PersonExt 객체로 복사함.
            정점에 전달된 메시지가 없을 때는 기존 PersonExt 객체를 그대로 반환함.
         */
        val graphAgg = newGraphExt.outerJoinVertices(aggVertices)(
            (_, originPerson, optMsg) => {
                optMsg match {
                    case Some(msg) => PersonExt(originPerson.name, originPerson.age, msg._1, msg._2, msg._3)
                    case None => originPerson
                }
            }
        )
        graphAgg.vertices.collect.foreach(println)

        /*
        그래프 부분 집합 선택
            - subgraph: 주어진 조건을 만족하는 정점과 간선 선택
                def subgraph(
                    epred: EdgeTriplet[VD, ED] => Boolean = (x => true),
                    vpred: (VertexId, VD) => Boolean = ((v, d) => true)
                ): Graph[VD, ED]

            - mask
                주어진 그래프에 포함된 정점만 선택
                그래프를 또 다른 그래프에 투영해 두 그래프에 모두 존재하는 정점과 간선만 유지함.
                메서드는 두 그래프의 속성 객체를 전혀 고려하지 않으며, 그래프 객체만 인수로 받음.

            - filter
                subgraph와 mask 메서드를 조합한 메서드
                전처리 함수, 정점 조건 함수, 간선 조건 함수를 인수로 받음.
                메서드는 전처리 함수를 사용해 원본 그래프를 또 다른 그래프로 변환한 후,
                정점 및 간선 조건 함수를 기준으로 변환한 그래프 중 일부를 선택함.
                선택한 그래프는 원본 그래프의 마스크로 사용함
                다시 말해 filter는 전처리 단계와 마스킹 단계를 한 번에 수행하는 메서드임.
                원본 그래프를 마스킹할 목적 외에는 전처리 그래프를 만들 이유가 없을 때 유용함.
         */

        // 자녀가 있는 사람만 선택
        val parents = graphAgg.subgraph(_ => true, (_, person) => person.children > 0)
        parents.vertices.collect.foreach(println)
        parents.vertices.collect.foreach(println)

        // 그래프 알고리즘
        // 최단 거리: 한 정점에서 다른 정점들로 향하는 최단 경로 찾기
        // 페이지 랭크: 정점으로 들어오고 나가는 간선 개수를 바탕으로 정점의 상대적 중요도를 계산함
        // 연결요소: 그래프에서 서로 완전히 분리된 서브그래프를 찾음
        // 강연결요소: 그래프에서 서로 연결된 정점들의 군집을 찾음

        // 예제 데이터셋
        // zipWithIndex를 사용해 각 문서에 고유 번호(ID)를 부여
        val articles = sc.textFile("articles.tsv").
            filter(x => x.trim() != "" && !x.startsWith("#")).
            zipWithIndex().cache()  // 문서 이름과 ID를 빠르게 검색할 수 있또록 articles RDD를 메모리에 저장

        val links = sc.textFile("links.tsv").
            filter(x => x.trim() != "" && !x.startsWith("#"))

        // links의 각 줄을 파싱해서 각 링크의 문서 이름을 얻은 후
        // 문서 이름과 articles RDD를 조인해 문서 이름을 문서 ID로 대체
        // linkIndex : (출발 문서 ID, 도착 문서 ID) 쌍의 튜플 저장.
        val linkIndex = links.map(x => {
            val spl = x.split("\t")
            (spl(0), spl(1))
        }).join(articles).map(x => x._2).join(articles).map(x => x._2)

        val wikigraph = Graph.fromEdgeTuples(linkIndex, 0)

        // 최단 경로 알고리즘

        // Rainbow 문서에서 14th_century 문서로 가는 최단 경로 찾기
        // 두 문서의 정점 ID 찾기
        articles.filter(x => x._1 == "Rainbow" || x._1 == "14th_century").
            collect().
            foreach(println)        // 14th_century: 10, Rainbow: 3425

        // 랜드마크까지 거리를 담은 Map 객체
        val shortest = ShortestPaths.run(wikigraph, Seq(10))

        // Rainbow 페이지에 해당하는 정점 찾기
        shortest.vertices.filter(x => x._1 == 3425).collect().foreach(println)  // (3425,Map(10 -> 2))

        // 페이지 랭크
        val ranked = wikigraph.pageRank(0.001)

        // 데이터셋에서 가장 중요한 페이지 열 개 찾기
        val ordering = new Ordering[(VertexId, Double)] {
            def compare(x: (VertexId, Double), y: (VertexId, Double)): Int = x._2.compareTo(y._2)
        }
        val top10 = ranked.vertices.top(10)(ordering)

        // articles RDD와 조인해 문서 이름 출력
        sc.parallelize(top10).join(articles.map(_.swap)).
            collect.sortWith((x, y) => x._2._1 > y._2._1).
            foreach(println)

        // 연결 요소(connected Components)
        // wikiCC 그래프의 정점 속성에는 해당 정점이 속한 연결요소의 가장 작은 정점 ID(즉, 연결요소의 ID)가 저장됨.
        // 이 연결요소 ID의 고유 목록을 계산하면 그래프의 모든 연결요소를 찾을 수 있음.
        // 또 이목록과 articles RDD를 다시 조인해 연결요소 ID의 페이지 이름을 조회할 수 있음.
        val wikiCC = wikigraph.connectedComponents()
        wikiCC.vertices.map(x => (x._2, x._2)).
            distinct().join(articles.map(_.swap)).collect.foreach(println)
        /*
            (0,(0,%C3%81ed%C3%A1n_mac_Gabr%C3%A1in))
            (1210,(1210,Directdebit))

            그래프가 두 군집으로 분리되어 있음을 알 수 있음.
         */

        // 각 군집의 페이지 개수 카운트
        wikiCC.vertices.map(x => (x._2, x._2)).countByKey().foreach(println)
        /*
            (0,4589)
            (1210,3)
         */

        // 강연결요소(Strongly Connected Components)
        // 연결요소 알고리즘과 마찬가지로 SCC 알고리즘이 반환한 그래프의 각 정점 속성에는
        // 해당 정점이 속한 강연결요소의 가장 작은 정점 ID가 저장됨.
        val wikiSCC = wikigraph.stronglyConnectedComponents(100)
        wikiSCC.vertices.map(x => x._2).distinct().count()

        // 어떤 SCC의 규모가 가장 큰지 알아보기
        wikiSCC.vertices.map(_.swap).countByKey().toList.
            sortWith((x, y) => x._2 > y._2).foreach(println)
        /*
            (6,4051)
            (2488,6)
            (1831,3)
            (892,2)
            (1950,2)
            (4224,2)
            (1111,2)
            (2474,2)
            ...
         */

        wikiSCC.vertices.filter(x => x._2 == 2488).
            join(articles.map(_.swap)).collect().foreach(println)
    }
}
