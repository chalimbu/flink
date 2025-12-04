package part3state

import generators.shopping.{AddToShoppingCartEvent, SingleShoppingCartEventsGenerator}
import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

object RichObjects {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  val numberStream: DataStream[Int]  =env.fromElements(1,2,3,4,5,6)

  val tenXNumber: DataStream[Int] = numberStream.map(_*10)

  // * explicit map functions
  val tenXNumber_v2: DataStream[Int] = numberStream.map(new MapFunction[Int,Int] {

    override def map(value: Int): Int = value * 10
  }
  )

  // Rich map function
  val tenXNumbers_v3: DataStream[Int] = numberStream.map(new RichMapFunction[Int,Int] {
    override def map(value: Int): Int = value*10
  })

  //richmapfunction + lifecycle methods
  val tenXNumbersWithLifecycle: DataStream[Int] = numberStream.map(new RichMapFunction[Int,Int] {
    override def map(value: Int): Int = value*10 // mandatory override
    // optional overrides: lifecycle methods open and close

    // called before data goes through
    override def open(parameters: Configuration): Unit  =
      println("Starting my work!!")

    override def close(): Unit = {
      println("finishing my work...")
    }

  })

  val tenXNumbersWithLifecycle2: DataStream[Int] = numberStream.map(new RichMapFunction[Int,Int] {
    override def map(value: Int): Int = value*10 // mandatory override
    // optional overrides: lifecycle methods open and close

    // called before data goes through
    override def open(parameters: Configuration): Unit  =
      println("Starting my 2 work!!")

    override def close(): Unit = {
      println("finishing my 2 work...")
    }

  })

  // ProcessFunction - the most general function abstration in flink
  val tenXNumbersProcess: DataStream[Int] = numberStream.process(new ProcessFunction[Int,Int] {

    override def processElement(value: Int, ctx: ProcessFunction[Int, Int]#Context, out: Collector[Int]): Unit = {
      out.collect(value*10)
    }

    override def open(parameters: Configuration): Unit = {
      println("process function starting")
    }

    override def close(): Unit = {
      println("closing process function")
    }
  })

  /**
   * Exercise: "explode" all purchases events to a single item
   * [("boots",2] (iphone,1)]
   * boots, boots, iphone
   * use whatever function you want lambad, rich function, process functions
   */

    def exercise(): Unit = {
      val exerciseEnv = StreamExecutionEnvironment.getExecutionEnvironment
      val shopingCartStream = exerciseEnv.addSource(new SingleShoppingCartEventsGenerator(100))
        .filter(_.isInstanceOf[AddToShoppingCartEvent])
        .map(_.asInstanceOf[AddToShoppingCartEvent])

      /*val withProcessFunction = shopingCartStream.process(new ProcessFunction[AddToShoppingCartEvent,String] {
        override def processElement(value: AddToShoppingCartEvent,
                                    ctx: ProcessFunction[AddToShoppingCartEvent, String]#Context,
                                    out: Collector[String]): Unit = {
          var i = 0
          while ( i<value.quantity){
            out.collect(value.sku)
            i = i+1
          }
        }
      }).print()*/

      exerciseEnv.execute()
    }


  def main(args: Array[String]): Unit = {
    //tenXNumbersWithLifecycle.print()

    //env.execute()
    exercise()
  }
}
