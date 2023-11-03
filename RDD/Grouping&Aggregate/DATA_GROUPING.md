# DATA GROUPING

```
데이터 그룹핑은 데이터를 특정 기준에 따라 단일 컬렉션으로 집계하는 연산을 의미함.
aggregateByKey, groupByKey(또는 groupBy), combineByKey 등 다양한 Pair RDD 변환 연산자로 데이터를 그룹핑할 수 있음.
```



## groupByKey, groupBy 변환 연산자로 데이터 그룹핑

```
groupByKey 변환 연산자는 동일한 키를 가진 모든 요소를 단일 키-값 쌍으로 모은 Pair RDD를 반환함.

(A,1)
(A,2)						(A, (1, 2))
(B,1)			->			(B, (1, 3))
(B,3)						(C, (1))
(C,1)

결과 RDD는 (K, Iterable[V]) 타입의 요소로 구성됨.
각 요소의 Iterable[V]로 해당 키의 모든 값에 반복문을 적용할 수 있음.

groupBy는 Pair RDD가 아닌 일반 RDD에서도 사용할 수 있으며,
일반 RDD를 Pair RDD로 변환하고 groupByKey를 호출하는 것과 같은 결과를 만들 수 있음.
예를 들어 임의의 T 타입 요소로 구성된 RDD[T]와
T 타입 요소에서 K 타입의 키를 생성하는 함수(f: T => K)를 사용해 다음과 같이 groupBy를 호출할 수 있음.

rdd.map(x => (f(x), x)).groupByKey()
rdd.groupBy(f)

groupByKey는 각 키의 모든 값을 메모리로 가져오기 때문에
이 메서드를 사용할 때는 메모리 리소스를 과다하게 사용하지 않도록 주의해야 함.
모든 값을 한꺼번에 그룹핑할 필요가 없으면(ex. 각 키별 평균을 계산할 때)
aggregateByKey나 reduceByKy, foldByKey를 사용하는 것이 좋음.
다른 Pair RDD의 변환 연산자와 마찬가지로 groupByKey와 groupBy 또한 파티션 개수를 받는 버전과
사용자 정의 Partitioner를 받는 버전을 추가로 제공함.
```



## combineByKey 변환 연산자로 데이터 그룹핑

```
combineByKey를 호출하려면 세 가지 커스텀 함수를 정의해 전달해야 함.
그 중 두 번째 함수는 Pair RDD에 저장된 값들을 결합 값으로 병합하며, 세 번째 함수는 결합 값을 최종 결과로 병합함.
또 메서드에 Partitioner를 명시적으로 지정해야 함.
마지막으로 mapSideCombine 플래그와 커스텀 Serializer를 선택 인수로 지정할 수 있음.

combineByKey[C](createCombiner: V => C,
	mergeValue: (C, V) => C,
	mergeCombiners: (C, C) => C,
	partitioner: Partitioner,
	mapSideCombine: Boolean = true,
	serializer: Serializer = null
	): RDD[(K, C)]
	
첫 번째 인수인 createCombiner는 각 파티션별로 키의 첫 번째 값(C)에서 최초 결합 값(V)을 생성하는 데 사용함.
두 번째 인수인 mergeValue는 동일 파티션 내에서 해당 키의 다른 값을 결합 값에 추가해 병합하는 데 사용함.
세 번째 인수인 mergeCombiners는 여러 파티션의 결합 값을 최종 결과로 병합하는 데 사용함.

Partitioner가 기존 Partitioner와 같을 때는
동일한 키를 가진 요소가 이미 동일한 파티션에 배치되어 있기 때문에 셔플링을 따로 실행할 필요가 없음.
따라서 셔플링이 발생하지 않으면 mergeCombiners 함수 또한 호출되지 않음.
(이것과 별개로 셔플링 단계에서 데이터를 디스크로 내보내는 기능을 사용하지 않을 때도 호출되지 않음.)
그렇지만 mergeCombiners 함수는 항상 전달해야 함.

마지막 두 선택 인자도 셔플링 실행 여부와 관련됨.
mapSideCombine 플래그(기본 값 : true)는 각 파티션의 결합 값을 계산하는 작업을 셔플링 단계 이전에 수행할지 지정함.
하지만 combineByKey가 셔플링 과정을 거치지 않을 때는 결합 값을 파티션 안에서만 계산하기 때문에 이 인자도 사용되지 않음.
마지막으로 (스파크의 spark.serializer 환경 매개변수에 지정된) 기본 Serializer를 사용하고 싶지 않을 때
커스텀 Serializer를 메서드에 전달할 수 있음.

combineByKey는 다양하고 유연한 데이터 그룹핑 기능을 제공함. 다른 그룹핑 연산자도 combineByKey를 사용함.
```

