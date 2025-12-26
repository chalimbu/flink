package part4io

import generators.shopping._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutputs {

  // shopping cart events
  // process this events in two differnte way with the same function
  // eg events for use "alice", and all the events of everyone else

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val shoppingCartEvent = env.addSource(new SingleShoppingCartEventsGenerator(100)) // 10 events per second

  //output tags - only available for processFunction
  val aliceTag = new OutputTag[ShoppingCartEvent]("alice-events") // name should be unique

  class AliceEventFunction extends ProcessFunction[ShoppingCartEvent,ShoppingCartEvent]{

    override def processElement(event: ShoppingCartEvent,
                                ctx: ProcessFunction[ShoppingCartEvent, ShoppingCartEvent]#Context,
                                out: Collector[ShoppingCartEvent] // "primary" destination
                               ): Unit = {
      if (event.userId == "Alice"){
        ctx.output(aliceTag,event)
      }else{
        out.collect(event)
      }
    }
  }

  def demoSideOutput():Unit = {
    val allEventsButAlices: DataStream[ShoppingCartEvent]= shoppingCartEvent.process(new AliceEventFunction())
    val alicesEvents: DataStream[ShoppingCartEvent] = allEventsButAlices.getSideOutput(aliceTag)

    // process the data stream separately
    alicesEvents.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    demoSideOutput()
  }

}
