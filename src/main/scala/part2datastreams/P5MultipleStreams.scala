package part2datastreams

import generators.shopping._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.co.{CoProcessFunction, ProcessJoinFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object P5MultipleStreams {

  /*
  - union
  - window join
  - interval join
  - connect

   */

  // Unioning = combining multiple streams into just one
  // mutiple of same type, union into 1
  def demoUnion(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val shopingCartEventsKafka  = env.addSource(new SingleShoppingCartEventsGenerator(300,sourceId = Some("kafka")))

    val shopingCartEventFiles = env.addSource(new SingleShoppingCartEventsGenerator(1000,sourceId = Some("files")))

    val combineShopingCartEventStream: DataStream[ShoppingCartEvent] = shopingCartEventsKafka.union(shopingCartEventFiles)

    combineShopingCartEventStream.print()
    env.execute()
  }

  // window join  = elements belong to the same window + some join condition
  def demoWindowJoin():Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val shopingCartStream = env.addSource(new SingleShoppingCartEventsGenerator(1000,sourceId = Some("kafka")))
    val catalogEvents = env.addSource(new CatalogEventsGenerator(200))

    val joinedStream = shopingCartStream
      .join(catalogEvents)
      // the join stream needs a join condition
      .where(shopingCartEvent => shopingCartEvent.userId)
      .equalTo(shopingCatalogEvent => shopingCatalogEvent.userId)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      // do something with the correlated events
      .apply((shopingCartEvent,catalogEvents) =>
        s"user ${shopingCartEvent.userId} browsed at ${catalogEvents.time} and bought at ${shopingCartEvent.time}")

      joinedStream.print()

    env.execute()
  }

  // interval join = correlation between events a and b if durationMin  < if timea - timeb < durationmax
  // involves even times
  // only works on keyed streams

  def intervalJoins(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // we need both streams to have event times
    val shoppingCartStream =
      env.addSource(new SingleShoppingCartEventsGenerator(1000,sourceId = Some("kafka")))
        .assignTimestampsAndWatermarks(
          WatermarkStrategy.forBoundedOutOfOrderness(java.time.Duration.ofMillis(500))
            .withTimestampAssigner(new SerializableTimestampAssigner[ShoppingCartEvent] {
              override def extractTimestamp(element: ShoppingCartEvent, recordTimestamp: Long): Long =
                element.time.toEpochMilli
            })
        ).keyBy(_.userId)
    val catalogEvents =
      env.addSource(new CatalogEventsGenerator(200))
        .assignTimestampsAndWatermarks(
          WatermarkStrategy.forBoundedOutOfOrderness(java.time.Duration.ofMillis(500))
            .withTimestampAssigner(new SerializableTimestampAssigner[CatalogEvent] {
              override def extractTimestamp(element: CatalogEvent, recordTimestamp: Long): Long =
                element.time.toEpochMilli
            })
        ).keyBy(_.userId)

    val intervalJoinStream = shoppingCartStream.intervalJoin(catalogEvents).between(Time.seconds(-2),Time.seconds(2))
      .lowerBoundExclusive()
      .lowerBoundExclusive() // by default the interval on top is inclusive
      .process(new ProcessJoinFunction[ShoppingCartEvent,CatalogEvent,String] {
        override def processElement(left: ShoppingCartEvent,
                                    right: CatalogEvent,
                                    ctx: ProcessJoinFunction[ShoppingCartEvent, CatalogEvent, String]#Context,
                                    out: Collector[String]): Unit = {
          out.collect(s" User ${left.userId} browsed at ${right.time} and bough at ${left.time}")
        }
      })

    intervalJoinStream.print()
    env.execute()
  }

  // connect = two stream are treated with the same "operator"
  def demoConnect(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setMaxParallelism(1)

    // two separated stream
    val shoppingCartStream = env.addSource(new SingleShoppingCartEventsGenerator(100)).setParallelism(1)
    val catalogEvents = env.addSource(new CatalogEventsGenerator(1000)).setParallelism(1)

    //connect stream
    val connectedStream: ConnectedStreams[ShoppingCartEvent,CatalogEvent] = shoppingCartStream.connect(catalogEvents)

    // variables - will use single-threaded
    val ratioStream = connectedStream.process(
      new CoProcessFunction[ShoppingCartEvent,CatalogEvent,Double] {
        var shoppinCartEventCount  = 0
        var catalogEventCount = 0

        override def processElement1(value: ShoppingCartEvent,
                                     ctx: CoProcessFunction[ShoppingCartEvent,
                                       CatalogEvent, Double]#Context, out: Collector[Double]): Unit = {
          shoppinCartEventCount += 1
          out.collect(shoppinCartEventCount*100/(shoppinCartEventCount+catalogEventCount))
        }

        override def processElement2(value: CatalogEvent,
                                     ctx: CoProcessFunction[ShoppingCartEvent, CatalogEvent, Double]#Context,
                                     out: Collector[Double]): Unit = {
          catalogEventCount += 1
          out.collect(shoppinCartEventCount*100/(shoppinCartEventCount+catalogEventCount))

        }
      }

    )

    ratioStream.print()
    env.execute()
  }


  def main(args: Array[String]): Unit = {
    demoConnect()
  }
}
