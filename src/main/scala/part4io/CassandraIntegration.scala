package part4io

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.cassandra.CassandraSink

object CassandraIntegration {

  // write data to cassandra
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  case class Person(name: String, age: Int) // exact schema of data in cassadra

  //write data to cassandra
  def demoWriteDataToCassandra(): Unit = {
    val people = env.fromElements(
      Person("Daniel",99),
      Person("Alice",12),
      Person("Julie",14),
      Person("Mom",54)
    )

    // we can only write tuples to casandra
    val personTuples: DataStream[(String,Int)] = people.map{ p => (p.name,p.age)}

    CassandraSink.addSink(personTuples)
      .setQuery("insert into rtjvm.people(name,age) values (?,?)")
      .setHost("localhost")
      .build()

    env.execute()
  }


  def main(args: Array[String]): Unit = {
    demoWriteDataToCassandra()
  }
}
