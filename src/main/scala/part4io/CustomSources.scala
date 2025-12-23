package part4io

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

import java.io.{BufferedReader, DataOutputStream, InputStreamReader, PrintStream}
import java.net.{ServerSocket, Socket, SocketAddress}
import scala.util.Random

object CustomSources {

  class RandomNumberGeneratorSource(minEventsPerSecond: Double)
  extends SourceFunction[Long] {

    // can create local fields and methods
    val maxSleepTime = (1000/minEventsPerSecond).toLong
    var isRunning: Boolean = true

    //called ONCE, runs on dedicate thread hence  okay to use Thread.sleep
    override def run(ctx: SourceFunction.SourceContext[Long]): Unit =
      while(isRunning){
        val sleepTime = Math.abs(Random.nextLong() % maxSleepTime)
        val nextNumber = Random.nextLong()
        Thread.sleep(sleepTime)

        // push something to the output
        ctx.collect(nextNumber)
      }

    // when running if cancel should inmmediately stop

    override def cancel(): Unit = {
      isRunning = false
    }
  }

  class RichRandomNumberGeneratorSource(minEventsPerSecond: Double)
    extends RichSourceFunction[Long] {

    // can create local fields and methods
    val maxSleepTime = (1000/minEventsPerSecond).toLong
    var isRunning: Boolean = true

    //called ONCE, runs on dedicate thread hence  okay to use Thread.sleep
    override def run(ctx: SourceFunction.SourceContext[Long]): Unit =
      while(isRunning){
        val sleepTime = Math.abs(Random.nextLong() % maxSleepTime)
        val nextNumber = Random.nextLong()
        Thread.sleep(sleepTime)

        // push something to the output
        ctx.collect(nextNumber)
      }

    // when running if cancel should inmmediately stop

    override def cancel(): Unit = {
      isRunning = false
    }

    override def open(parameters: Configuration): Unit = {
      println(s"[${Thread.currentThread().getName}] starting source function" )
    }

    override def close(): Unit ={
      println(s"[${Thread.currentThread().getName}] closing source function")
    }

    // can hold state - ValueState, ListState, MapState
  }

  class RichParallelRandomNumberGeneratorSource(minEventsPerSecond: Double)
    extends RichParallelSourceFunction[Long] {

    // can create local fields and methods
    val maxSleepTime = (1000/minEventsPerSecond).toLong
    var isRunning: Boolean = true

    //called ONCE per thread, each instance has it's own thread
    override def run(ctx: SourceFunction.SourceContext[Long]): Unit =
      while(isRunning){
        val sleepTime = Math.abs(Random.nextLong() % maxSleepTime)
        val nextNumber = Random.nextLong()
        Thread.sleep(sleepTime)

        // push something to the output
        ctx.collect(nextNumber)
      }

    // when running if cancel should inmmediately stop

    override def cancel(): Unit = {
      isRunning = false
    }

    override def open(parameters: Configuration): Unit = {
      println(s"[${Thread.currentThread().getName}] starting source function" )
    }

    override def close(): Unit ={
      println(s"[${Thread.currentThread().getName}] closing source function")
    }

    // can hold state - ValueState, ListState, MapState
  }

  def demoSourceFunction(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val numberStream: DataStream[Long] = env.addSource(new RichParallelRandomNumberGeneratorSource(10)).setParallelism(10)
    numberStream.print()
    env.execute()
  }

  /**
   *  create a source fucntion that read data from a socket
   *
   */

    class SocketStringSource(host: String,port: Int) extends SourceFunction[String] {

    var running = true;

    override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
      val socket = new Socket(host,port)
      while (running){

        val dataOuput = new BufferedReader(new InputStreamReader(socket.getInputStream))
        ctx.collect(dataOuput.readLine())
      }
      socket.close()
    }

    override def cancel(): Unit = {
      running = false
    }
  }

  def exerciseSocket(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val numberStream: DataStream[String] = env.addSource(new SocketStringSource("localhost",12345))
    numberStream.print()
    env.execute()
  }



  def main(args: Array[String]): Unit = {
    exerciseSocket
  }
}

/*
  - stard DataSender
  - start flink
  - Datasender -> Flink
 */
object DataSender {
  def main(args: Array[String]): Unit = {
    val serverSocket = new ServerSocket(12345)
    println("waiting for flink to connect to port 12345")
    val socket = serverSocket.accept()
    println("flink connected sending data")
    val printer = new PrintStream(socket.getOutputStream)
    printer.println("Hello from the other side")
    Thread.sleep(3000)
    printer.println("Almost ready")
    Thread.sleep(5000)
    (1 to 10).foreach( i=> {
      Thread.sleep(200)
      printer.println(s"Number $i")
    })
    println("Data sending complete")
    serverSocket.close()
  }
}
