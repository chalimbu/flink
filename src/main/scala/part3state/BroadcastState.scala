package part3state

import generators.shopping._
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object BroadcastState {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val shoppingCartEvents = env.addSource(new SingleShoppingCartEventsGenerator(100))
  val eventByUser = shoppingCartEvents.keyBy(x=>x.userId)

  // issue a waning if quantity > threshold

  def purchasedWarning(): Unit = {
    val threshold = 2

    val notificationStream = eventByUser
      .filter(_.isInstanceOf[AddToShoppingCartEvent])
      .filter(_.asInstanceOf[AddToShoppingCartEvent].quantity > threshold)
      .map( event => event match {
        case AddToShoppingCartEvent(userId, sku, quantity, time) =>
          s" User $userId attemping to purchase $quantity items of $sku when threshold is $threshold"

      })

    notificationStream.print()
    env.execute()
  }

  // how to change the threshold overtime while the app runs
  // the threshold can be broacasted, broadcast state

  def changingThreshold(): Unit = {
    val threshold: DataStream[Int] = env.addSource(new SourceFunction[Int]{
      override def run(ctx: SourceFunction.SourceContext[Int]): Unit =
        List(2,0,4,5,6,3).foreach{ newThreshold =>
          Thread.sleep(1000)
          ctx.collect(newThreshold)
        }
      override def cancel(): Unit = ()
    })

    // broadcast state is always a map
    val broadcastStateDescriptor = new MapStateDescriptor[String,Int]("thresholds",classOf[String],classOf[Int])
    val broadcastThresholds: BroadcastStream[Int]= threshold.broadcast(broadcastStateDescriptor)

    val notificationStream = eventByUser
      .connect(broadcastThresholds)
      // key, input stream, broadcast input, output
      .process(new KeyedBroadcastProcessFunction[String,ShoppingCartEvent,Int,String] {

        var thresholdDescriptor:MapStateDescriptor[String,Int] = _

        override def open(parameters: Configuration): Unit = {
          thresholdDescriptor = new MapStateDescriptor[String,Int]("thresholds",classOf[String],classOf[Int])
        }

        override def processBroadcastElement(newThreshold: Int,
                                             ctx: KeyedBroadcastProcessFunction[String, ShoppingCartEvent, Int, String]#Context,
                                             out: Collector[String]): Unit = {
          println(s"threshold about to be change --- $newThreshold")
          // fetch the broadcast state = distributed variable
          val stateThreshold = ctx.getBroadcastState(thresholdDescriptor) // loading the broadcast state into current state
          //update the state
          stateThreshold.put("quantity-threshold", newThreshold)
        }

        override def processElement(event: ShoppingCartEvent,
                                    ctx: KeyedBroadcastProcessFunction[String, ShoppingCartEvent, Int, String]#ReadOnlyContext,
                                    out: Collector[String]): Unit = {
          event match {
            case AddToShoppingCartEvent(userId, sku, quantity, time) => {
              val hasState = ctx.getBroadcastState(thresholdDescriptor).contains("quantity-threshold")
              val currentThreshold: Int = if(hasState){
                ctx.getBroadcastState(thresholdDescriptor).get("quantity-threshold")
              }else {
                0
              }
              if(quantity>currentThreshold){
                out.collect(s" User $userId attemping to purchase $quantity items of $sku when threshold is $threshold")
              }
            }
            case _ =>
          }
        }


      })
    notificationStream.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    changingThreshold()
  }
}
