object higherOrderFunc2 {
    def main(args: Array[String]): Unit = {
        println(TransferMoney(100, bankFee))
    }

    // 콜백 함수
    // 콜백 함수는 어떤 함수에 파라미터로 전달될 수 있는 함수
    def TransferMoney(money: Double, bankFee: Double => Double): Double = {
        money + bankFee(money)
    }
    def bankFee(amount: Double) = amount * 0.05
}