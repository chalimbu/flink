package part3state

import generators.shopping._
import org.apache.flink.api.common.state.{ListStateDescriptor, _}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object KeyedState {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val shopingCartEvents = env.addSource(new SingleShoppingCartEventsGenerator(
    sleepMillisBetweenEvents = 100,
    generateRemoved = true
  ))

  def demoValueState(): Unit = {


    /*
    * How many event per user have been generated?
    * in total up to that point
    * */

    val eventsPerUser: KeyedStream[ShoppingCartEvent,String] = shopingCartEvents.keyBy(_.userId)

    val numEventsPerUserNaive = eventsPerUser.process(
      //key,event,output
      new KeyedProcessFunction[String,ShoppingCartEvent,String] {
        var nEventsForThisUser = 0 // this is wrong because is local(other nodes don't see it) and if hte node dies non restore

        override def processElement(value: ShoppingCartEvent,
                                    ctx: KeyedProcessFunction[String, ShoppingCartEvent, String]#Context,
                                    out: Collector[String]): Unit = {
          nEventsForThisUser += 1
          out.collect(s" User ${value.userId} - $nEventsForThisUser")
        }
      }
    )

    val numEventPerUserStream = eventsPerUser.process(new KeyedProcessFunction[String,ShoppingCartEvent,String] {

      // .value to get value, and .update for new value
      var stateCounter: ValueState[Long] = _

      override def open(parameters: Configuration): Unit = {
        //initialize all state
        stateCounter = getRuntimeContext.getState(new ValueStateDescriptor[Long]("stateCounter", classOf[Long]))

      }

      override def processElement(value: ShoppingCartEvent,
                                  ctx: KeyedProcessFunction[String, ShoppingCartEvent, String]#Context,
                                  out: Collector[String]): Unit = {
        val currentStateCounter = stateCounter.value() +1
        stateCounter.update(currentStateCounter)
        out.collect(s" User ${value.userId} - ${currentStateCounter}")
      }
    })

    numEventPerUserStream.print()

    env.execute()

  }

  def demoListState():Unit = {
    // store all the events per user id
    val allEventPerUserStream: DataStream[String] = shopingCartEvents.keyBy(_.userId).process(
      new KeyedProcessFunction[String, ShoppingCartEvent, String] {

        /*
        add(value)
        addAll(list)
        update( with new list)
         */
        var stateEventForUser: ListState[ShoppingCartEvent] = _

        override def open(parameters: Configuration): Unit = {
          stateEventForUser = getRuntimeContext.getListState(new
              ListStateDescriptor[ShoppingCartEvent]("stateEventForUser", classOf[ShoppingCartEvent])
          )
        }

        override def processElement(event: ShoppingCartEvent,
                                    ctx: KeyedProcessFunction[String, ShoppingCartEvent, String]#Context,
                                    out: Collector[String]): Unit = {
          stateEventForUser.add(event)

          import scala.collection.JavaConverters._ // implicit converter  (extension methods)
          val currentEvents: Iterable[ShoppingCartEvent] = stateEventForUser.get().asScala // - not a list but a java iterable

          out.collect(s"User ${event.userId} - [${currentEvents.mkString(",")}")
        }
      }
    )

    allEventPerUserStream.print()

    env.execute()

  }

  // MapState
  def demoMapState(): Unit = {
    // count how many events per type, per user
    val streamCountPerType: DataStream[String] = shopingCartEvents.keyBy(_.userId).process(
      new KeyedProcessFunction[String,ShoppingCartEvent,String] {

        var stateCountPerEventType: MapState[String,Long] = _

        override def open(parameters: Configuration): Unit = {
          stateCountPerEventType = getRuntimeContext.getMapState(
            new MapStateDescriptor[String,Long]("stateCountPerEventType",classOf[String],classOf[Long])
          )
        }

        override def processElement(event: ShoppingCartEvent,
                                    ctx: KeyedProcessFunction[String, ShoppingCartEvent, String]#Context,
                                    out: Collector[String]): Unit = {
          // fetch type of event
          val eventType = event.getClass.getSimpleName
          if(stateCountPerEventType.contains(eventType)){
            stateCountPerEventType.put(eventType,stateCountPerEventType.get(eventType)+1)
          }else{
            stateCountPerEventType.put(eventType,1L)
          }

          // push some output
          import scala.collection.JavaConverters._ // implicit converter  (extension methods)
          out.collect(s"${event.userId} - ${stateCountPerEventType.entries().asScala.mkString(",")}")

        }
      }
    )
    streamCountPerType.print()
    env.execute()
  }

  // the state can be cleare manually or with time

  def demoListStateWithClearance():Unit = {
    // store all the events per user id
    val allEventPerUserStream: DataStream[String] = shopingCartEvents.keyBy(_.userId).process(
      new KeyedProcessFunction[String, ShoppingCartEvent, String] {
        import scala.collection.JavaConverters._ // implicit converter  (extension methods)

        var stateEventForUser: ListState[ShoppingCartEvent] = _

        override def open(parameters: Configuration): Unit = {
          val descriptor = new ListStateDescriptor[ShoppingCartEvent]("stateEventForUser", classOf[ShoppingCartEvent])
          descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.hours(1))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)// this means will return old value in the time between clean request
            // and actual remove from disk
          .build())
          stateEventForUser = getRuntimeContext.getListState(descriptor
          )
        }

        override def processElement(event: ShoppingCartEvent,
                                    ctx: KeyedProcessFunction[String, ShoppingCartEvent, String]#Context,
                                    out: Collector[String]): Unit = {
          stateEventForUser.add(event)
          val currentEvents = stateEventForUser.get().asScala.toList
          if(currentEvents.size>10){
            stateEventForUser.clear() // clearing is not done inmmediately
          }

          out.collect(s"User ${event.userId} - [${currentEvents.mkString(",")}")
        }
      }
    )

    allEventPerUserStream.print()

    env.execute()

  }


  def main(args: Array[String]): Unit = {
    demoListStateWithClearance()
  }
}
