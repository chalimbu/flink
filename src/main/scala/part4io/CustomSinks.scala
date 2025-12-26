package part4io

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

import java.io.{FileWriter, PrintStream, PrintWriter}
import java.net.{ServerSocket, Socket}
import java.util.Scanner

object CustomSinks {

  val env  = StreamExecutionEnvironment.getExecutionEnvironment
  val stringStream = env.fromElements(
    "this is an example of a sing function",
    "some other string",
    "daniel say that this is ok"
  )

  // push the string to a file sink, instanciate once per thread
  class FileSink(path: String) extends RichSinkFunction[String]{

    var writter: PrintWriter = _
    override def open(parameters: Configuration): Unit = {
      writter = new PrintWriter(new FileWriter(path,true))// append mode
    }

    override def close(): Unit = {
      writter.close()
    }

    override def invoke(event: String, context: SinkFunction.Context): Unit = {
      try{
      writter.println(event)
      writter.flush()
      }catch {
        case e: Exception => println(s"exception ${e.getMessage}")
      }
      }

  }

  def demoFileSink(): Unit = {
    stringStream.addSink(new FileSink("output/demoFileSink.txt"))
    stringStream.print()
    env.execute()
  }

  /***
   *  create a sink function that will push data (as strings) to a socket sink
   *
   */
    class SocketSink(host: String, port: Int) extends RichSinkFunction[String] {

    //var serverSocket: ServerSocket = _
    var socket: Socket = _
    var printer: PrintWriter = _

    override def open(parameters: Configuration): Unit = {
      socket = new Socket(host,port)
      printer = new PrintWriter(socket.getOutputStream)

    }

    override def invoke(event: String, context: SinkFunction.Context): Unit = {
      printer.println(event)
      printer.flush()
    }

    override def close(): Unit = {
      socket.close()
    }


  }

 def demoSocketSink(): Unit = {
   stringStream.addSink(new SocketSink("localhost",12345))//.setParallelism(1)
   stringStream.print()
   env.execute()
 }

  def main(args: Array[String]): Unit = {
    demoSocketSink()
  }

}

object DataReceiver {
  def main(args: Array[String]): Unit = {
    val server = new ServerSocket (12345)
    println("waiting for flink to connect.....")
    val socket = server.accept()
    val reader = new Scanner(socket.getInputStream)
    println("Flink connected reading")

    while(reader.hasNextLine){
      println(s" > ${reader.nextLine()}")
    }

    socket.close()
    println("all data read closing app")
    server.close()
  }
}
