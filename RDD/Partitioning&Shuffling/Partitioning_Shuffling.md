# Partitioning & Shuffling

```
RDD는 데이터셋을 나타내는 불변 컬렉션이고, 안정성과 장애 복구 기능이 있음.
RDD는 단일 데이터셋이 아닌 데이터를 기반으로 동작함.
즉, RDD는 클러스터 전체에 분산된 파티션 데이터를 관리함.
따라서 데이터 파티셔닝의 개념은 스파크 잡의 기능에 중대한 영향을 미칠 뿐 아니라
성능에 큰 영향을 미칠 수 있으며, 자원에 대한 활용 방법까지도 영향을 미침.

RDD는 데이터 파티션으로 구성되고 모든 연산은 RDD의 데이터 파티션에서 수행됨.
프랜스포메이션과 같은 여러 연산은 데이터를 조작하는 특정 파티션에서 익스큐터에 의해 실행되는 함수.
그러나 개별 익스큐터가 데이터 파티션에서 격리된 연산을 수행한다고 해서 모든 연산이 수행되는 것은 아님.
집계와 같은 연산에서는 셔플링 스테이지에서는 클러스터 전체 데이터가 이동되어야 함.

파티션 개수는 RDD 트랜스포메이션을 실행할 태스크 수에 직접적인 영향을 주기 때문에 중요함.
파티션 개수가 너무 적으면 많은 데이터에서 아주 일부의 CPU/코어만 사용하기 때문에
성능이 저하되고 클러스터를 제대로 활용하지 못하게 됨.
반면에 파티션 개수가 너무 많으면 실제로 필요한 것보다 많은 자원을 이용하기 때문에
멀티 테넌트 환경에서는 자원 부족 현상이 발생할 수 있음.
```



## Spark Partitioner

```
파티셔너에 의해 RDD 파티셔닝이 진행됨. 파티셔너는 파티션 인덱스를 RDD 엘리먼트에 할당함.
동일 파티션에 존재하는 모든 엘리먼트는 동일한 파티션 인덱스를 가질 것임.

스파크는 Partitioner 구현체로 HashPartitioner와 RangePartitioner를 제공함.
또 사용자 정의 Partitioner를 Pair RDD에 사용할 수 있음.
```



### 1) HashPartitioner

```
HashPartitioner는 스파크의 기본 Partitioner.
각 요소의 자바 해시 코드를 단순한 mod 공식에 대입해 파티션 번호를 계산함.

partitionIndex = hashcode(key) % numPartitions

각 요소의 파틴션 번호를 거의 무작위로 결정하기 때문에 모든 파티션을 정확하게 같은 크기로 분할할 가능성이 낮음.
하지만 대규모 데이터셋을 상대적으로 적은 수의 파티션으로 나누면 대체로 데이터를 고르게 분산시킬 수 있음.

HashPartitioner를 사용할 경우 데이터 파티션의 기본 개수는 스파크의 spark.default.parallelism 환경 매개변수 값으로 결정됨.
이 매개변수를 지정하지 않으면 스파크는 클러스터의 코어 개수를 대신 사용함.
```



### 2) RangePartitioner

```
RangePartitioner는 정렬된 RDD의 데이터를 거의 같은 범위 간격으로 분할할 수 있음.
범위는 모든 파티션의 시작 키와 종료 키를 알고 있어야 하기 때문에 RangePartition을 사용하기 전에 RDD를 먼저 정렬해야 함.

먼저 RDD를 기반으로 하는 파티션에 대한 합리적인 경계를 필요로 하고
키 K부터 특정 엘리먼트가 속하는 partitionIndex까지의 함수를 생성함.
마지막에는 결정한 범위에 RDD 엘리먼트를 올바르게 배포하기 위해 RangePartitioner를 기반으로 RDD를 리파티셔닝해야 함.
```



### 3) Pair RDD의 사용자 정의 Partitioner

```
파티션(또는 파티션을 처리하는 태스크)의 데이터를 특정 기준에 따라 정확하게 배치해야 할 경우
사용자 정의 Partitioner로 Pair RDD를 분할할 수 있음.
예를 들어 각 태스크가 특정 키-값 쌍 데이터만 처리해야 할 때 사용자 정의 Partitioner를 쓸 수 있음.
사용자 정의 Partitioner는 Pair RDD에만 쓸 수 있으며,
Pair RDD의 변환 연산자를 호출할 때 사용자 정의 Partitoner를 인수로 전달함.
대부분의 Pair RDD 변환 연산자는 두 가지 추가 버전을 제공함.
첫 번째 버전은 Int 인수(변경할 파티션 개수)를 추가로 받으며,
두 번째 버전은 사용할 Partitioner(스파크 지원 Partitioner 또는 사용자 정의 Partitioner)를 추가 인수로 받음.
파티션 개수를 받는 메서드를 호출하면 기본 Partitioner인 HashPartitioner를 사용함.

mapValues와 flatMapValues를 제외한 Pair RDD의 변환 연산자는 모두 이 두가지 버전의 메서드를 추가로 제공함.
mapValues와 flatMapValues는 항상 파티셔닝을 보존함.

Pair RDD 변환 연산자를 호출할 때 Partitioner를 따로 지정하지 않으면
스파크는 부모 RDD에 지정된 파티션 개수 중 가장 큰 값을 사용함.
Partitioner를 정의한 부모 RDD가 없다면 spark.default.parallelism 매개변수에 지정된 파티션 개수로 HashPartitoner를 사용함.

또 기본 HashPartitioner를 그대로 사용하면서 임의의 알고리즘으로 키의 해시 코드만 바꾸어도
Pair RDD 데이터의 파티션 배치를 변경할 수 있음.
일부 활용 사례에서는 이 방법이 구현하기 더 쉽고, 부주의한 셔플링을 피할 수 있어 성능도 향상할 수 있음.
```

---



## 불필요한 셔플링 줄이기

```
스파크의 셔플링은 파티션 간의 물리적인 데이터 이동을 의미함.
서플링은 새로운 RDD의 파티션을 만들려고 여러 파티션의 데이터를 합칠 때 발생함.
예를 들어 키를 기준으로 요소를 그룹핑하려면 스파크는 RDD의 파티션을 모두 살펴보고 키가 같은 요소를 전부 찾은 후,
이를 물리적으로 묶어서 새로운 파티션을 구성하는 과정을 수행해야 함.

셔플링은 스파크 잡의 실행 프로세스를 결정할뿐만 아니라 잡이 스테이지로 분할되는 부분에 영향을 미침.
스파크는 RDD의 계보를 나타내는 DAG를 갖고 있음.
스파크 잡 실행을 계획하기 위해 계보를 사용할뿐만 아니라 익스큐터에 장애가 발생해도 바로 복구할 수 있음.
RDD에서 트랜스포메이션 함수가 실행할 때는 동일 노드에서 데이터에 대해 연산이 수행되게 해야 함.
그러나 의도적인 또는 의도하지 않은 리파티셔닝을 유발하는 연산 중 조인 연산, 리듀스, 그룹핑, 집계 연산을 종종 사용함.
해당 셔플링은 차례대로 처리의 특정 스테이지가 끝나고 새로운 스테이지가 시작될 부분을 결정함.

셔플링이 많을수록 스파크 잡이 실행될 때 더 많은 스테이지가 발생하기 때문에 성능에 영향을 미침.
RDD의 두 가지 의존성 타입인 좁은 의존성과 넓은 의존성은 스파크 드라이버에서 스테이지를 결정하게 함.

- 좁은 의존성 (잡 실행의 동일한 스테이지에 있음)
    filter, map, flatMap 함수 등처럼 간단한 일대일 트랜스포메이션을 사용해
    특정 RDD를 다른 RDD로 생성할 수 있을 때 자식 RDD는 부모 RDD와 일대일로 의존한다고 말할 수 있음.
    해당 의존성은 데이터가 여러 익스큐터 간 데이터를 전송하지 않은 채 
    RDD/부모 RDD 파티션을 포함하는 노드와 동일한 노드에서 변환될 수 있기 때문에 좁은 의존성으로 불림.

- 넓은 의존성 (잡을 실행할 때 새로운 스테이지가 도입됨)
    데이터를 전달하거나 교환하기 위해 aggregateByKey, reduceByKey 등과 같은 함수를 사용해
    데이터를 리파티셔닝하거나 재분배해서 하나 이상의 RDD로 보낼 수 있을 때
    자식 RDD는 셔플링 연산에 참여하는 부모 RDD에 의존한다고 말할 수 있음.
    이런 의존성은 원 RDD/부모 RDD 파티션을 포함하는 노드에서 데이터를 변환할 수 없음.
    여러 익스큐터 간에 데이터를 전달해야 하기 때문에 넓은 의존성으로 부름.

셔플링 바로 전에 수행한(변환 함수) 태스크를 맵 태스크라고 하며,
바로 다음에 수행한(병합 함수) 태스크를 리듀르 태스크라고 함.
맵 태스크의 결과는 중간 파일에 기록하며(주로 운영체제의 파일 시스템 캐시에만 저장),
이후 리듀스 태스크가 이 파일을 읽어 들임. 중간 파일을 디스크에 기록하는 작업도 부담이지만,
결국 셔플링할 데이터를 네트워크로 전송해야 하기 때문에 스파크 job의 셔플링 횟수를 최소한으로 줄이도록 해야 함.

RDD 변환 연산에는 대부분 셔플링이 필요하지 않지만, 일부 연산에서는 특정 조건하에 셔플링이 발생하기도 함.
```



### 1) 셔플링 발생 조건1 : Partitioner를 명시적으로 변경하는 경우

```
Pair RDD 변환 연산자의 대부분에 사용자 정의 Partitioner를 지정할 수 있음.
사용자 정의 Partitioner를 쓰면 반드시 셔플링이 발생함.

또 이전 HashPartitioner와 다른 HashPartitoner를 사용해도 셔플링이 발생함.
스파크는 HashPartitioner 객체가 다르더라도 동일한 파티션 개수를 지정했다면 같다고 간주함.
(파티션 개수가 같은 HashPartitioner는 동일한 요소에서 항상 동일한 파티션 번호를 선택함)
다시 말해 이전에 사용한 HashPartitioner와 파티션 개수가 다른 HashPartitioner를 변환 연산자에 사용하면 셔플링이 발생함.
결국 Partitioner를 명시적으로 변경하면 셔플링이 발생한다고 볼 수 있음.
그러므로 가급적이면 기본 Partitioner를 사용해 의도하지 않은 셔플링은 최대한 피하는 것이 성능 면에서 가장 안전함.
```



### 2) 셔플링 발생 조건2 : Partitioner를 제거하는 경우

```
변환 연산자에 Partitioner를 명시적으로 지정하지 않았는데도 간혹 셔플링이 발생할 때가 있다.
대표적으로 map과 flatMap은 RDD의 Partitioner를 제거함.
이 연산자 자체로는 셔플링을 발생시키지 않지만, 연산자의 결과 RDD에 다른 변환 연산자(ex. aggregateByKey)를 사용하면
기본 Partitioner를 사용했더라도 여전히 셔플링이 발생함.
Pair RDD의 키를 변경하지 않는다면 map, flatMap 대신 mapValues, flatMapValues를 사용해 Partitioner를 보존하는 것이 성능상 좋음.

map이나 flatMap 변환 연산자 뒤에 사용하면 셔플링이 발생하는 변환 연산자
	- RDD의 Partitioner를 변경하는 Pair RDD 변환 연산자
		: aggregateByKey, foldByKey, reduceByKey, groupByKey, join, leftOuterJoin,
		  rightOuterJoin, fullOuterJoin, subtractByKey
	
	- 일부 RDD 변환 연산자 : subtract, intersection, groupWith
	
	- sortByKey 변환 연산자(무조건 발생)
	
	- partitionBy 또는 shuffle=true로 설정한 coalesce 연산자
```



### 3) 외부 셔플링 서비스로 셔플링 최적화

> [공식 문서](https://spark.apache.org/docs/3.3.0/configuration.html#shuffle-behavior)

```
셔플링을 수행하면 익스큐터는 다른 익스큐터의 파일을 읽어 들여야 함(셔플링은 pulling 방식을 사용)
하지만 셔플링 도중 일부 익스큐터에 장애가 발생하면
해당 익스큐터가 처리한 데이터를 더 이상 가져올 수 없어서 데이터 흐름이 중단됨.

반면 외부 셔플링 서비스는 익스큐터가 중간 셔플 파일을 읽을 수 있는 단일 지점을 제공해
셔플링의 데이터 교환 과정을 최적화할 수 있음.
외부 셔플링 서비스를 활성화 하면, 스파크는 각 워커 노드별로 외부 셔플링 서버를 시작함.
spark.shuffle.service.enable -> true
```

---



## RDD 파티션 변경

```
작업 부하를 효율적으로 분산시키거나 메모리 문제를 방지하려고 RDD의 파티셔닝을 명시적으로 변경해야 할 때가 있음.
예를 들어 일부 스파크 연산자에는 파티션 개수의 기본 값이 너무 작게 설정되어 있어
이 값을 그대로 사용하면 파티션에 매우 많은 요소를 할당하고 메모리를 과다하게 점유해 결과적으로 병렬 처리 성능이 저하될 수 있음.
RDD의 파티션을 변경할 수 있는 변환 연산자에는
partitionBy, coalesce, repartition, repartitionAndSortWithinPartition이 있음.
```



### 1) partitionBy

```
Pair RDD에서만 사용할 수 있고 파티셔닝에 사용할 Partitioner 객체만 인자로 전달할 수 있음.
전달된 Partitioner가 기존과 동일하면 파티셔닝을 그대로 보존하고, RDD도 동일하게 유지함.
반면 Partitioner가 기존과 다르면 셔플링 작업을 스케줄링하고 새로운 RDD를 생성함.
```



### 2) coalesce와 repartition

```
coalesce 연산자는 파티션 개수를 줄이거나 늘리는 데 사용.

coalesce(numPartitions: Int, shuffle: Boolean = false)

두 번째 인자는 셔플링의 수행 여부를 지정함.
파티션 개수를 늘리려면 이 shuffle 인자를 true로 설정해야 함.
반면 파티션 개수를 줄일 때는 이 인자를 false로 설정할 수 있음.
coalesce 메서드 알고리즘은 새로운 파티션 개수와 동일한 개수의 부모 RDD 파티션을 선정하고
나머지 파티션의 요소를 나누어 선정한 파티션과 병합하는 방식으로 파티션 개수를 줄임.
또 이 과정에서 부모 RDD 파티션의 지역성 정보를 최대한 유지하면서 데이터를 전체 파티션에 고르게 분배하려고 노력함.
repartition 변환 연산자는 단순히 shuffle을 true로 설정해 coalesce를 호출한 결과를 반환함.

한 가지 유의할 점은 coalesce의 shuffle 인자를 false로 지정한 경우
셔플링이 발생하지 않는 coalesce 이전의 모든 변환 연산자는
coalesce에 새롭게 지정된 익스큐터 개수(즉, 파티션 개수)를 사용한다는 것임.
반대로 true로 지정하면 coalesce 이전의 변환 연산자들은 원래의 파티션 개수를 그대로 사용하며,
이후 연산자들만 새롭게 지전된 파티션 개수로 실행함.
```



### 3) repartitionAndSortWithinPartition

```
이 연산자는 정렬 가능한 RDD, 즉 정렬 가능한 키로 구성된 Pair RDD에서만 사용할 수 있음.

새로운 Partitioner 객체를 받아 각 파티션 내에서 요소를 정렬함.
이 연산자는 셔플링 단계에서 정렬 작업을 함께 수행하기 때문에
repartition을 호출한 후 직접 정렬하는 것보다 성능이 더 낫다고 볼 수 있음.
셔플링을 항상 수행함.
```

---



## 파티션 단위로 데이터 매핑

```
스파크에서는 RDD의 전체 데이터뿐만 아니라 RDD의 각 파티션에 개별적으로 매핑 함수를 적용할 수도 있음.
이 메서드를 잘 활용하면 각 파티션 내에서만 데이터가 매핑되도록 기존 변환 연산자를 최적화해 셔플링을 억제할 수 있음.
파티션 단위로 동작하는 RDD 연산에는 mapParitions와 mapPartitionsWithIndex, 파티션을 요소로 매핑하는 glom 등이 있음.
```



### 1) mapPartitions, mapPartitionsWithIndex

```
mapPartitions는 매핑 함수를 인수로 받는다는 점에서 map과 동일하지만,
매핑 함수를 Iterator[T] => Iterator[U]의 시그니처로 정의해야 한다는 점이 다름.
mapPartitions의 매핑 함수는 각 파티션의 모든 요소를 반복문으로 처리하고 새로운 RDD 파티션을 생성함.

mapPartitionsWithIndex 또한 유사하지만, 매핑 함수에 파티션 번호가 함께 전달된다는 점이 다름.
(Int, Iterator[T]) => Iterator[U]
매핑 함수는 이 파티션 번호를 매핑 작업에 활용할 수 있음.

매핑 함수에는 입력 Iterator를 새로운 Iterator로 변환하는 코드를 구현해야 함.
스칼라는 다음과 같이 다양한 Iterator 관련 함수를 제공함.
	- map, flatMap, zip, zipWithIndex 등 함수를 Iterator의 각 요소에 적용할 수 있음.
	- take(n)이나 takeWhile(condition)을 사용해 일부 요소를 가져올 수 있음
	- drop(n)이나 dropWhile(condition)을 사용해 일부 요소를 제외할 수 있음.
	- filter로 요소를 필터링할 수 있음.
	- slice(m, n)을 사용해 입력 Iterator의 부분 집합을 가져올 수 있음.
이 함수들은 모두 새로운 Iterator를 생성하므로 매핑 함수의 반환 값에 그대로 사용할 수 있음.

두 연산자는 preservePartitioning 인자를 추가로 받을 수 있음(기본값 : false)
이 값을 true로 설정하면 부모 RDD의 파티셔닝이 새로운 RDD에도 그대로 유지됨.
반대로 이 값을 false로 설정하면 연산자는 RDD의 Partitioner를 제거하며, 이후 다른 변환 연산에서 셔플링이 발생함.

스파크는 map에 전달된 매핑 함수가 데이터를 어떻게 변환하는지 따로 검사하지 않으며,
이 변환이 기존 파티셔닝을 무효로 만드는지도 알 수 없음. 사용자가 판단했을 때 매핑 함수가 기존 파티셔닝을 잘 유지한다면
직접 preserverPatitioning을 true로 설정해 성능을 최적화할 수 있음.

파티션 단위로 데이터를 매핑하면 파티션을 명시적으로 고려하지 않는 다른 일반 변환 연산자보다 일부 문제를 더 효율적으로 해결할 수 있음.
예를 들어 매핑 함수에 연산이 많이 필요한 작업(예: 데이터베이스 커넥션 생성 등)을 구현할 때는
각 요소별로 매핑 함수를 호출하는 것보다 파티션당 한 번씩 호출하는 편이 훨씬 더 좋음.
```



### 2) 파티션의 데이터를 수집하는 glom 변환 연산자

```
glom 연산자는 각 파티션의 모든 요소를 배열 하나로 모으고, 이 배열들을 요소로 포함하는 새로운 RDD를 반환함.
따라서 새로운 RDD에 포함된 요소 개수는 이 RDD의 파티션 개수와 동일함.
glom 연산자는 기존의 Partitioner를 제거함.
```

```scala
val list = List.fill(500)(scala.util.Random.nextInt(100))
// List[Int] = List(93, 70, 70, 92, 76, 43, 56, ...)

val rdd = sc.parallelize(list, 30).glom()
// org.apache.spark.rdd.RDD[Array[Int]] = MapPartitionsRDD[7] at glom at <console>:24

rdd.collect()
// Array[Array[Int]] = Array(Array(93, 70, 70, 92, 76, 43, 56, 24, 73, 57, 63, 14, ...), ...)

rdd.count()
// Long = 30
```

```
파티션 30개에 분산된 RDD를 생성한 후 glom 연산자를 호출함.
출력 결과를 살펴보면 glom이 반환한 RDD 요소는 각 파티션의 데이터로 구성된 배열이며,
배열 객체의 개수 또한 30개라는 것을 알 수 있음.

glom을 잘 활용하면 RDD를 손쉽게 단일 배열로 만들 수 있음.
우선 RDD의 파티션 개수를 한 개로 만든 후 glom을 호출하면 기존 RDD의 모든 요소를 포함한 단일 배열이 RDD 형태로 반환됨.
물론 모든 요소를 단일 파티션에 저장할 수 있을 만큼 데이터가 작아야 함.
```

