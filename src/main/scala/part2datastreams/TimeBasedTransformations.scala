package part2datastreams

// _ to process cases clases and implicits
import generators.shopping._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, Watermark, WatermarkGenerator, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}

import java.time.Instant

object TimeBasedTransformations {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

  val shoppingCartEvents  =env.addSource(new ShoppingCartEventsGenerator(sleepMillisPerEvent = 100,
    batchSize = 5, // events in groups of 5
    baseInstant = Instant.parse("2022-02-15T00:00:00.000Z")// start
  ))

  // 1. Event time,  the time the event was created
  // 2. processing time, the moment the event arrives at flink

  class CountByWindowAll extends ProcessAllWindowFunction[ShoppingCartEvent,String,TimeWindow] {

    override def process(context: Context, elements: Iterable[ShoppingCartEvent], out: Collector[String]): Unit = {
      val window = context.window
      out.collect(s"Window [${window.getStart} - ${window.getEnd}] ${elements.size}")
    }
  }

  /*
  Group by window, every 3 second, tumbling(non overlaping)
   */
  /*
With processing time
- we don't care when the event was created
- multiple runs generate different results
 */
  def demoProcessingTime(): Unit = {
    def groupEventsByWindow = shoppingCartEvents
      .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(3)))
    def countEventsByWindows: DataStream[String] = groupEventsByWindow
      .process(new CountByWindowAll())

    countEventsByWindows.print()
      env.execute()

  }

  /*
  With event time
  - we need to care about handling late data
  - we don't care about flink internal state
  - we might see faster results
  - same events + different runs = same result
   */
  def demoEventTime(): Unit = {
    val groupedEventsByWindows = shoppingCartEvents
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness(java.time.Duration.ofMillis(500)) // max delay < 500 milis
          .withTimestampAssigner(new SerializableTimestampAssigner[ShoppingCartEvent] {
            override def extractTimestamp(element: ShoppingCartEvent, recordTimestamp: Long): Long = element.time.toEpochMilli
          })
      ).windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))
    val countEventsByWindows: DataStream[String] = groupedEventsByWindows
      .process(new CountByWindowAll())

    countEventsByWindows.print()

    env.execute()
  }

  /*
  Custom watermarks
   */
  // with every new max timestamps, every new incoming element with event time < max timestamp - max delay will be discarded
  class BoundedOutOrdernessGenerator(maxDelay: Long) extends WatermarkGenerator[ShoppingCartEvent] {
    var currentMaxTimestamp: Long = 0L

    // when a new events is processed
    // every new event older than this in absolute time will be discarded
    // emiting a watermarks is not mandatory
    override def onEvent(event: ShoppingCartEvent, eventTimestamp: Long, output: WatermarkOutput): Unit =
      currentMaxTimestamp = Math.max(currentMaxTimestamp,event.time.toEpochMilli)

    // to maybe emit watermarks regularly, can be called without actual events passing
    override def onPeriodicEmit(output: WatermarkOutput): Unit =
      output.emitWatermark(new Watermark(currentMaxTimestamp-maxDelay-1))
  }

  def demoEventTime_v2(): Unit = {
    // to control how ofter flink call onperiodic emit

    env.getConfig.setAutoWatermarkInterval(1000L)// call on periodic emit every 1 second

    val groupedEventsByWindows = shoppingCartEvents
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.
          forGenerator( _ => new BoundedOutOrdernessGenerator(500L))
          .withTimestampAssigner(new SerializableTimestampAssigner[ShoppingCartEvent] {
            override def extractTimestamp(element: ShoppingCartEvent, recordTimestamp: Long): Long = element.time.toEpochMilli
          })
      ).windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))
    val countEventsByWindows: DataStream[String] = groupedEventsByWindows
      .process(new CountByWindowAll())

    countEventsByWindows.print()

    env.execute()
  }
  /*
  14> Window [1644883200000 - 1644883203000] 5
1> Window [1644883203000 - 1644883206000] 5
2> Window [1644883209000 - 1644883212000] 5
3> Window [1644883215000 - 1644883218000] 5
4> Window [1644883218000 - 1644883221000] 5
5> Window [1644883224000 - 1644883227000] 5
6> Window [1644883230000 - 1644883233000] 5
7> Window [1644883233000 - 1644883236000] 5
8> Window [1644883239000 - 1644883242000] 5

1> Window [1644883200000 - 1644883203000] 5
2> Window [1644883203000 - 1644883206000] 5
3> Window [1644883209000 - 1644883212000] 5
4> Window [1644883215000 - 1644883218000] 5
5> Window [1644883218000 - 1644883221000] 5
6> Window [1644883224000 - 1644883227000] 5
7> Window [1644883230000 - 1644883233000] 5
8> Window [1644883233000 - 1644883236000] 5
9> Window [1644883239000 - 1644883242000] 5
   */
  def main(args: Array[String]): Unit = {
    demoEventTime_v2
  }
}
