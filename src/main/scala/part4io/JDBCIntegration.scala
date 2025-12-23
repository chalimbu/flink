package part4io

import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.scala._

import java.sql.PreparedStatement

object JDBCIntegration {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  case class Person(name: String,age: Int)
  // write data to JDBC

  def jvmDemoWriteToJDBC(): Unit = {
    val persons = env.fromElements(
      Person("Daniel",99),
      Person("Alberto",14),
      Person("Miguel",32),
      Person("Augusto",24)
    )

    val jdbcSink = JdbcSink.sink[Person](
      // 1 - SQL statement
      "insert into people (name,age) values (?,?)",
      new JdbcStatementBuilder[Person] {
        override def accept(statement: PreparedStatement, person: Person): Unit = {
          statement.setString(1,person.name) // first willcard with person .name
          statement.setInt(2,person.age)
        }
      },
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl("jdbc:postgresql://localhost:5432/rtjvm")
        .withDriverName("org.postgresql.Driver")
        .withUsername("docker")
        .withPassword("docker")
        .build()
    )

    persons.addSink(jdbcSink)
    persons.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    jvmDemoWriteToJDBC()
  }

}
