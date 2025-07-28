package part2datastreams

import generators.shopping.{ShoppingCartEvent, ShoppingCartEventsGenerator}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, PurgingTrigger}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object P4Triggers {

  // Triggers -> when a window function is executed

  val env  = StreamExecutionEnvironment.getExecutionEnvironment

  def demoFirstTrigger(): Unit ={
    val shopingCartEvents: DataStream[String] = env.addSource(new ShoppingCartEventsGenerator(
      sleepMillisPerEvent = 500, batchSize = 2 // 2 events per second)
    )).windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))) // 10 events per window
      .trigger(CountTrigger.of[TimeWindow](5))
      .process(new CountByWindowAll)

    shopingCartEvents.print()

    env.execute()
  }


  // purging trigger
  def demoPurgingrigger(): Unit ={
    val shopingCartEvents: DataStream[String] = env.addSource(new ShoppingCartEventsGenerator(
        sleepMillisPerEvent = 500, batchSize = 2 // 2 events per second)
      )).windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))) // 10 events per window
      .trigger(PurgingTrigger.of(CountTrigger.of[TimeWindow](5)))// runs every 5 second, and clears the windo
      .process(new CountByWindowAll)

    shopingCartEvents.print()

    env.execute()
  }

  def main(args: Array[String]): Unit = {
    demoPurgingrigger()
  }

  // copy from time base transformation
  class CountByWindowAll extends ProcessAllWindowFunction[ShoppingCartEvent,String,TimeWindow] {

    override def process(context: Context, elements: Iterable[ShoppingCartEvent], out: Collector[String]): Unit = {
      val window = context.window
      out.collect(s"Window [${window.getStart} - ${window.getEnd}] ${elements.size}")
    }
  }

  /*
  Other triggers
  - EventTimeTrigger: happens by default when the waterwark is > windows end time (automatic for event time windows)
  - ProcessingTimeTrigger: fires when the current system time > window end time (automatic for processing time windows)
  - custom triggers: powerful api for custom firing behaviour


   */
}
