object AnonymousFunc {
    def main(args: Array[String]): Unit = {
        def TransferMoney(money: Double, bankFee: Double => Double): Unit = {
            println(money + bankFee(money))
        }
        TransferMoney(100, (amount: Double) => amount * 0.05)
    }
}