package part3state

import generators.shopping._
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state.{CheckpointListener, ValueState, ValueStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object Checkpoints {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // set checkpoint intervals
  env.getCheckpointConfig.setCheckpointInterval(5000) // a checkpoint triggered every 5 s
  // set checkpoint storage
  env.getCheckpointConfig.setCheckpointStorage("file:///Users/sebastian.zapata/Insync/sebitaszapata12@gmail.com/Google Drive/pc-personal/copia/estudio/flink/rock-the-jvm-flink/flink/checkpoints")

  /*
   Keep track of the number of AddToCart events, when quantity > a threshold (e.g. managing stock)
   Persist the data (state) via checkpoints
   */

  val shoppingCartEvents = env.addSource(
    new SingleShoppingCartEventsGenerator(sleepMillisBetweenEvents = 100, generateRemoved = true)
  )

  val eventsByUser = shoppingCartEvents
    .keyBy(_.userId)
    .flatMap(new HighQuantityCheckpointedFunction(5))

  def main(args: Array[String]): Unit = {
    eventsByUser.print()
    env.execute()
  }
}

class HighQuantityCheckpointedFunction(val threshold: Long)
  extends FlatMapFunction[ShoppingCartEvent, (String,Long)]
    with CheckpointedFunction
    with CheckpointListener
    {

  override def flatMap(event: ShoppingCartEvent, out: Collector[(String, Long)]): Unit = {
    event match {
      case AddToShoppingCartEvent(userId, sku, quantity, time) =>
        if(quantity > threshold){
          val newUserEventCount = stateCount.value() + 1
          stateCount.update(newUserEventCount)
          // push output
          out.collect((userId,newUserEventCount))
        }
      case _ => ()
    }
  }

      var stateCount: ValueState[Long] = _ // instatetiate per key

      override def snapshotState(context: FunctionSnapshotContext): Unit = {
        println(s"CHECKPOINT AT ${context.getCheckpointTimestamp}")
      }

      // lyfecile method analog to initialize state in open RichFunctions
      override def initializeState(context: FunctionInitializationContext): Unit = {
        val stateCountDescriptor = new ValueStateDescriptor[Long]("imposibleOrderCount", classOf[Long])
        stateCount = context.getKeyedStateStore.getState(stateCountDescriptor)
      }

      override def notifyCheckpointComplete(checkpointId: Long): Unit = ()

      override def notifyCheckpointAborted(checkpointId: Long): Unit = ()
    }
