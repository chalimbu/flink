package part2datastreams

import org.apache.flink.streaming.api.scala._

object EssentialStream {

  def applicationTemplate (): Unit = {
    // 1 - execution enviroment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //computations in between
    import org.apache.flink.streaming.api.scala._ // import TypeInformation for the data of your datastreams
    val simpleNumberStream: DataStream[Int] = env.fromElements(12,3,4,5)



    simpleNumberStream.print()
    // it print the thread number and then the number itself

    // a the end
    env.execute()

  }

  // transformations
  def demoTransformation(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val numbers: DataStream[Int] = env.fromElements(1,2,3,4,5)

    //checking parallelism
    println(s"current parallelims ${env.getParallelism}")
    // set paralleims
    env.setParallelism(2)
    println(s"current parallelims ${env.getParallelism}")


    // map
    val doubleNumbers = numbers.map(_*2)

    // flatmap
    val expandedNumber = numbers.flatMap(n => List(n, n+1, n+2))

    // filter
    val filteredNumbers = numbers
      .filter( _ % 2 == 0)
      .setParallelism(4)

    val finalData = expandedNumber.writeAsText("output/expandedString.txt") // directory with as many files as threads
    finalData.setParallelism(3)

    env.execute()
  }

  def fizzbuzz(): Unit= {
    /**
     * exercise : fizzbuzz on flink
     * - take a stream of 100 natural number
     * - for every number
     *  - if n%3 == then return "fizz"
     *  - if n%5 == then "buzz"
     *  - if both => "fizzbuzz"
     * - print the number for wich you said "fizzbuzz" to a file or in the console.
     */

    // 1 - execution enviroment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //computations in between
    import org.apache.flink.streaming.api.scala._ // import TypeInformation for the data of your datastreams
    val input: DataStream[Int] = env.fromSequence(1,100).map(_.toInt).map{
      it =>
        var fizzbuzz=""
        if(it%3==0){
          fizzbuzz += "fizz"
        }
        if(it%5==0){
          fizzbuzz+="buzz"
        }
        Tuple2.apply(it,fizzbuzz)
    }.filter( it => it._2 =="fizzbuzz").map(_._1)

    val file = input.writeAsText("output/fizzbuzz.txt")
    file.setParallelism(2)

    val print = input.print()
    // it print the thread number and then the number itself

    // a the end
    env.execute()
  }



  def main(args: Array[String]): Unit = {
    fizzbuzz()
  }

}
