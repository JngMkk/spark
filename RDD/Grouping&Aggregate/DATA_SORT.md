# DATA SORT

```
RDD의 데이터를 정렬하는 데 주로 사용하는 변환 연산자는 repartitionAndSortWithinPartition, sortByKey, sortBy가 있음.
repartitionAndSortWithinPartition은 리파티셔닝과 정렬 작업을 동시에 수행하며,
두 작업을 개별적으로 수행하는 것보다 성능이 더 좋음.

sortBy 사용법은 매우 간단함.
	val sortedProds = totalsAndProds.sortBy(_._2._2(1))

키가 복합 객체라면 정렬 작업이 약간 까다로울 수 있음.
암시적 변환으로 키-값 튜플로 구성된 RDD에서만 Pair RDD의 변환 연산자를 사용할 수 있는 것처럼,
sortByKey와 repartitionAndSortWithinPartition도 정렬 가능한 클래스를 Pair RDD 키로 사용할 때만 호출할 수 있음.

스칼라에서는 Ordered trait 또는 Ordering trait을 이용해 클래스를 정렬 가능하게 만들고,
이 클래스를 키 또는 값을 가진 RDD를 정렬할 수 있음.
```



## Ordered trait으로 정렬 가능한 클래스 생성

```
정렬 가능한 클래스를 만드는 첫 번째 방법은 스칼라의 Ordered trait을 확장하는 것임.
Ordered를 확장하려면 동일한 클래스의 객체를 인수로 받아 this 객체와 비교하는 compare 함수를 구현해야 함.
compare 함수는 호출된 객체(this 객체)가 인수로 받은 객체(비교 대상)보다 더 클 때는 양의 정수를 반환하고,
호출된 객체가 더 작을 때는 음의 정수를 반환하고, 두 객체가 동일하면 0을 반환함.

sortByKey 변환 연산자를 사용하려면 Ordering 타입의 인수가 필요하지만,
스칼라가 암시적 변환으로 Ordered를 Ordering으로 바꾸기 때문에 Ordered를 사용해도 안전하게 SortByKey를 호출할 수 있음.

예를 들어 다음 케이스 클래스를 RDD 키로 사용해 직원들의 성을 기준으로 요소를 정렬할 수 있음.

case class Employee(lastName: String) extends Ordered[Employee] {
	override def compare(that: Employee) = this.lastName.compare(that.lastName)
}
```



## Ordering trait으로 정렬 가능한 클래스 생성

```
정렬 가능한 클래스를 만드는 두 번째 방법은 Ordering trait을 사용하는 것.
예를 들어 Ordered 예제와 달리 Employee 클래스를 수정할 수 없으므로 Ordered trait도 확장할 수 없지만,
여전히 직원들의 성을 기준으로 데이터를 정렬해야 한다고 가정함.
이때 sortByKey를 호출하는 함수의 스코프 내에 Ordering[Employee] 타입의 객체를 다음과 같이 정의할 수 있음.

implicit val emplOrdering = new Ordering[Employee] {
	override def compare(a: Employee, b:Employee) = a.lastName.compare(b.lastName)
}

또는

implicit val emplOrdering: Ordering[Employee] = Ordering.by(_.lastName)

이처럼 암시적 객체를 스코프 내에 정의하고 Employee 타입의 키로 구성된 RDD의 sortByKey 변환 연산자를 호출하면,
sortByKey는 이 암시적 객체를 사용해 RDD를 정렬할 수 있음.

앞서 상품 이름으로 totalsAndProd RDD를 정렬한 코드가 아무런 사전 작업 없이도 동작한 이유는
기본 타입에 대한 Ordering 객체가 스칼라의 표준 라이브러리에 포함되어 있기 때문임.
하지만 복합 객체를 키로 사용하려면 여기서 설명한 방법으로 Ordering을 구현해야 함.
```



## 이차 정렬

```
각 키별로 값을 정렬해야 할 때가 있음.
예를 들어 고객 ID로 구매 기록을 그룹핑한 후 구매 시각을 기준으로 각 고객의 구매 기록을 정렬하는 작업이 필요할 수 있음.

이 때 groupByKeyAndSortValues 변환 연산자가 작업에 적합할 수 있음.
이 연산자를 호출하려면 (K, W) 쌍으로 구성된 RDD와 Ordering[V] 타입의 암시적 객체를 스코프 내에 준비하고,
연산자에 Partitioner 객체나 파티션 개수를 전달해야 함.

groupByKeyAndSortValues는 (K, Iterable(V)) 타입의 RDD를 반환하며,
Iterable(V)는 암시적 Ordering 객체에서 정의한 기준으로 정렬된 값들을 포함함.
하지만 이 메서드는 정렬 작업에 앞서 키별로 값을 그룹핑하기 때문에 상당한 메모리 및 네트워크 리소스를 사용함.

또는 다음과 같이 그룹핑 연산 없이도 이차 정렬을 효율적으로 수행할 수 있음.
	1. RDD[(K, V)]를 RDD[((K, V), null)]로 매핑
		ex. rdd.map(kv => (kv, null))
	2. 사용자 정의 Paritioner를 이용해 새로 매핑한 복합 키(K, V)에서 K 부분만으로 파티션을 나누면
		같은 K가 있는 요소들을 동일한 파티션에 모을 수 있음.
	3. repartitionAndSortWithinPartition 변환 연산자에 사용자 정의 Partitioner를 인수로 전달해 연산자 호출.
		이 연산자는 전체 복합 키(K, V)를 기준으로 각 파티션 내 요소들을 정렬함.
		복합 키로 요소를 정렬하는 순서는 먼저 키를 정렬 기준으로 쓴 후 값을 사용.
	4. 결과 RDD를 다시 RDD[(K, V)]로 매핑함.

repartitionAndSortWihtinPartition 연산을 활용해
각 파티션의 데이터를 키를 기준으로 정렬한 후 값을 기준으로 정렬할 수 있음.
그런 다음 결과 RDD에 mapPartitions를 호출해 정렬된 결과에 반복문을 적용할 수 있음.
이 방법은 값을 그룹핑하지 않기 때문에 성능 면에서 더 우수함.
```



## top과 takeOrdered로 정렬된 요소 가져오기

```
takeOrdered(n)이나 top(n) 행동 연산자를 사용해 RDD 쌍위 또는 하위 n개 요소를 가져올 수 있음.
임의의 T타입 요소로 구성된 RDD에서 상위 또는 하위 요소는 스코프 내에 정의된 암시적 Ordering[T] 객체를 기준으로 결정됨.

따라서 Pair RDD에서 top과 takeOrdered는 키를 기준으로 요소를 정렬하는 것이 아니라 (K, V) 튜플을 기준으로 요소를 정렬함.
Pair RDD에 이 메서드를 사용하려면 암시적 Ordering[(K, V)] 객체를 스코프 내에 정의해야 함.
(키와 값이 기본 타입일 때는 이미 Ordering 객체가 정의되어 있음)

top과 takeOrdered는 전체 데이터를 정렬하지 않음.
그 대신 각 파티션에서 상위(또는 하위) n개 요소를 가져온 후 이 결과를 병합하고, 이 중 상위(또는 하위) n개 요소를 반환함.
따라서 top과 takeOrdered 연산자는 훨씬 더 적은 양의 데이터를 네트워크로 전송하며,
sortBy와 take를 개별적으로 호출하는 것보다 무척 빠름.
하지만 collect와 마찬가지로 모든 겨로가를 드라이버의 메모리로 가져오기 때문에 n에 너무 큰 값을 지정해서는 안 됨.
```

