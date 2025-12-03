package part2datastreams

import generators.shopping._
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala._

object Partitions {



    def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment

      val shoppingCartEvents: DataStream[ShoppingCartEvent] =
        env.addSource(new SingleShoppingCartEventsGenerator(100) ) // around 10 events per seconds

      // partitioner = logic to split the data
      val partitioner = new Partitioner[String] {

        //num partitions depends on the number of cores in the pc
        override def partition(key: String, numPartitions: Int): Int = { // invoke in every event
          // hashcode % number of partition - even distribution
          println(s"Number of partitions $numPartitions")
          key.hashCode % numPartitions
        }
      }


      val partitionStream = shoppingCartEvents.partitionCustom(
        partitioner,event => event.userId
      )

      //partitionStream.print()


      /*
      * this is bad becasue you're only usig 1 task, risk of overloading with disproportionate data
      *
      * god for a task that interacts with the outside world, sending an http request(you don't want all task slots to send request)
      * */
      val badPartitioner = new Partitioner[String] {
        override def partition(key: String, numPartitions: Int): Int = {
          numPartitions-1
        }

      }
      val badPartitionStream = shoppingCartEvents
        .partitionCustom(badPartitioner, event =>event.userId)
        // redistribute data evenly- involves data transfer through network calls
        .shuffle

      badPartitionStream.print()


      env.execute()

    }
}
