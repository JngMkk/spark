object CallBack {
    def TransferMoney(money: Double) = {
        if (money > 1000)
            (money: Double) => "Dear customer, we are going to add the following amount as fee: " + money * 0.05
        else
            (money: Double) => "Dear customer, we are going to add the following amount as fee: " + money * 0.1
    }
    def main(args: Array[String]): Unit = {
        val returnedFunction = TransferMoney(1500)
        println(returnedFunction(1500))
    }
}