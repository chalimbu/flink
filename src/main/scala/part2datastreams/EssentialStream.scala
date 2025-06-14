package part2datastreams

import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction, ReduceFunction}
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object EssentialStream {

  def applicationTemplate(): Unit = {
    // 1 - execution enviroment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //computations in between
    import org.apache.flink.streaming.api.scala._ // import TypeInformation for the data of your datastreams
    val simpleNumberStream: DataStream[Int] = env.fromElements(12, 3, 4, 5)


    simpleNumberStream.print()
    // it print the thread number and then the number itself

    // a the end
    env.execute()

  }

  // transformations
  def demoTransformation(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val numbers: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5)

    //checking parallelism
    println(s"current parallelims ${env.getParallelism}")
    // set paralleims
    env.setParallelism(2)
    println(s"current parallelims ${env.getParallelism}")


    // map
    val doubleNumbers = numbers.map(_ * 2)

    // flatmap
    val expandedNumber = numbers.flatMap(n => List(n, n + 1, n + 2))

    // filter
    val filteredNumbers = numbers
      .filter(_ % 2 == 0)
      .setParallelism(4)

    val finalData = expandedNumber.writeAsText("output/expandedString.txt") // directory with as many files as threads
    finalData.setParallelism(3)

    env.execute()
  }

  def fizzbuzz(): Unit = {
    /**
     * exercise : fizzbuzz on flink
     * - take a stream of 100 natural number
     * - for every number
     *  - if n%3 == then return "fizz"
     *  - if n%5 == then "buzz"
     *  - if both => "fizzbuzz"
     *    - print the number for wich you said "fizzbuzz" to a file or in the console.
     */

    // 1 - execution enviroment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //computations in between
    import org.apache.flink.streaming.api.scala._ // import TypeInformation for the data of your datastreams
    val input: DataStream[Int] = env.fromSequence(1, 100).map(_.toInt).map {
      it =>
        var fizzbuzz = ""
        if (it % 3 == 0) {
          fizzbuzz += "fizz"
        }
        if (it % 5 == 0) {
          fizzbuzz += "buzz"
        }
        Tuple2.apply(it, fizzbuzz)
    }.filter(it => it._2 == "fizzbuzz").map(_._1)

    //val file = input.writeAsText("output/fizzbuzz.txt")

    val fileSink: SinkFunction[Int] = StreamingFileSink.forRowFormat(
      new Path("output/streamingfizzbuzz"),
      new SimpleStringEncoder[Int]("UTF-8")
    ).build()

    input.addSink(
      fileSink
    ).setParallelism(2)


    val print = input.print()
    // it print the thread number and then the number itself

    // a the end
    env.execute()
  }

  def explicitTransformation(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val input: DataStream[Long] = env.fromSequence(1, 100)

    // map
    val doubleNumbers = input.map(_ * 2)

    //explicit version
    val doubleNumbers_v2 = input.map(new MapFunction[Long, Long] {
      // declare fields, methods, ....

      override def map(value: Long): Long = value * 2
    })

    val expandedNumbers = input.flatMap(n => (1 to n.toInt).toList)

    //explicit version
    val expandedNumbers_v2 = input.flatMap(new FlatMapFunction[Long, Long] {
      override def flatMap(value: Long, out: Collector[Long]): Unit = (1 to value.toInt).foreach(x => out.collect(x))
    })

    //expandedNumbers.print()

    // process method
    // process function is the most general function to proces elements in flink
    val expandednumber_v3 = input.process(new ProcessFunction[Long, Long] {
      override def processElement(value: Long, ctx: ProcessFunction[Long, Long]#Context, out: Collector[Long]):
      Unit = (1 to value.toInt).foreach(x => out.collect(x))
    })

    // reduce
    // hapens on keyed streams -> hashmaps that are streaming
    val keyedNumber: KeyedStream[Long,Boolean]= input.keyBy(n => n%2==0)
    // using FP
    val sumByKey = keyedNumber.reduce(_+_) // sum of odd and event numbers
    // explicit approach
    val sumByKey_explicit = keyedNumber.reduce(new ReduceFunction[Long] {
      override def reduce(x: Long, y: Long): Long = x + y
    })

    sumByKey_explicit.print()
    // this did not print what i expect it prints for each key, and emits so 0,1,2,3,4 -> 0-true, 1-false, 2-true, 3-false
    // and will generate when it receive 0-true, will print 0, then receive 1-false, will print 1, tthe receive 2-true
    // and print 2, then it receives 3-false and will print 4(sum of 1 and 3)

    env.execute()
  }


  def main(args: Array[String]): Unit = {
    explicitTransformation()
  }

}
