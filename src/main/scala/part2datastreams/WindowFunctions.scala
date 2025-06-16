package part2datastreams

import generators.gaming._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessAllWindowFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Instant
import scala.concurrent.duration._

object WindowFunctions {

  // use-case: stream of events for a gaming session

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  //import org.apache.flink.streaming.api.scala._

  implicit val serverStartTime: Instant = Instant.parse("2022-02-02T00:00:00.000Z")
  val events: List[ServerEvent] = List(
    bob.register(2.seconds), // player name "bob" registred 2s after the server started
    bob.online(2.seconds),
    sam.register(3.seconds),
    sam.online(4.seconds),
    rob.register(4.seconds),
    alice.register(4.seconds),
    mary.register(6.seconds),
    mary.online(6.seconds),
    carl.register(8.seconds),
    rob.online(10.seconds),
    alice.online(10.seconds),
    carl.online(10.seconds)
  )

  val eventStream: DataStream[ServerEvent] = env.fromCollection[ServerEvent](events)
    .assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(java.time.Duration.ofMillis(500))
        // this means that after receiving and event with time T, discard events with T-500ms
        .withTimestampAssigner(new SerializableTimestampAssigner[ServerEvent] {
          override def extractTimestamp(element: ServerEvent, recordTimestamp: Long): Long = {
            element.eventTime.toEpochMilli
          }
        })
    )

  // how many player were registered every 3 seconds?
  // 0 to 3, 3 to 6s, 6 and 9s
  val threeSecondsTumblingWindow = eventStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))

  // count by windowAll
  class CountByWindowAll extends AllWindowFunction[ServerEvent, String, TimeWindow] {
    override def apply(window: TimeWindow, input: Iterable[ServerEvent], out: Collector[String]): Unit = {
      val registrationEventCount = input.count(events => events.isInstanceOf[PlayerRegistered])
      out.collect(s"Window [${window.getStart} - ${window.getEnd}] $registrationEventCount")
    }
  }

  def demoCountByWindow(): Unit = {
    val registrationsPerThreeSeconds: DataStream[String] = threeSecondsTumblingWindow.apply(new CountByWindowAll)
    registrationsPerThreeSeconds.print()
    env.execute()
  }

  // process offfers a richer function, wiht a lower level api akka acces to context vs the one on top
  class CountByWindowV2 extends ProcessAllWindowFunction[ServerEvent,String,TimeWindow] {

    override def process(context: Context, elements: Iterable[ServerEvent], out: Collector[String]): Unit = {
      val window = context.window
      val registrationEventCount = elements.count(events => events.isInstanceOf[PlayerRegistered])
      out.collect(s"Window [${window.getStart} - ${window.getEnd}] $registrationEventCount")
    }
  }

  def demoCountByWindow_v2(): Unit = {
    val registrationsPerThreeSeconds: DataStream[String] = threeSecondsTumblingWindow.process(new CountByWindowV2)
    registrationsPerThreeSeconds.print()
    env.execute()
  }

  // alternative 2: aggregate function
  class CountByWindowV3 extends AggregateFunction[ServerEvent,Long,Long] {
    //                                            input,    accumulator, output

    // start counting from 0
    override def createAccumulator(): Long = 0L

    // every element increases accumulator by 1
    override def add(value: ServerEvent, accumulator: Long): Long = {
      if(value.isInstanceOf[PlayerRegistered]){
        return accumulator+1
      }else {
        return accumulator
      }
    }

    // push final output out of hte final result
    override def getResult(accumulator: Long): Long = accumulator

    // accumu1 + accum2 = get bigger accumulator
    override def merge(a: Long, b: Long): Long = ???
  }

  def demoCountByWindow_v3(): Unit = {
    val registrationsPerThreeSeconds: DataStream[Long] = threeSecondsTumblingWindow.aggregate(new CountByWindowV3)
    registrationsPerThreeSeconds.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    demoCountByWindow_v3()
  }
}
