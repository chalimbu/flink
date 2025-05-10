package part1recap

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object ScalaRecap {

  val aBoolean: Boolean = false
  var aVariable: Int = 56
  aVariable += 1

  // expressions
  val anIfExpression: String = if (2>3) "bigger" else "smaller"

  // instructions vs expression
  val theUnit: Unit = println("Hello, Scala") // Unit === "void"

  // OOP
  class Animal
  class Cat extends Animal
  trait Carnivore{//interface
    def eat(animal: Animal): Unit
  }

  // inheritance: extends <= 1 class, but inherite from >= 0 traits

  class Crocodile extends Animal with Carnivore{
    override def eat(animal : Animal): Unit = println("eating this poor fellow")
  }

  // singleton
  object MySingleton

  // companion
  object Carnivore

  case class Person ( name : String, age: Int)// to string, serializable

  // generics
  class Mylist[A]

  // method notation
  // croc.eat(animal) or croc eat animal
  val tree = 1 + 2
  val tree_v2 = 1.+(2)

  // FP
  val incrementer: Int => Int = x => x +1
  val incremented = incrementer(4) // 5, same incrementer.apply(4)

  // map flatMap filter = HOFs
  val processedList = List(1,2,3).map(incrementer) // 2,3,4
  val aLongerList = List(1,2,3).flatMap(x=>List(x,x+1)) // 1,2,2,3,3,4

  // for-comprehesions
  val checkerBoard = List(1,2,3).flatMap(n => List('a','b','c').map(c => (n,c)))
  val checkerBoardv2 = for{
    n <- List(1,2,3)
    c <- List('a','b','c')
  } yield (n,c)

  // options and try
  val anOption: Option[Int] = Option (/*something that might be null*/ 43)
  val doubleOption = anOption.map(_*2)

  val anAttempt: Try[Int] = Try(12)
  val modifyAttempt = anAttempt.map(_+2)

  // pattern matching
  val anUnknow: Any = 45
  val medal = anUnknow match {
    case 1 => "gold"
    case 2 => "silver"
    case 3 => "bronze"
    case _ => "no medal"
  }

  val optionDescription = anOption match {
    case Some(value) => s" the option is not empty $value"
    case None => "the option is empty"
  }

  // Futures
  implicit val ec : ExecutionContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(8))
  val aFuture = Future (/*something to be evaluate in another thread*/ 1 + 999)

  // register callback when it finsihes
  aFuture.onComplete{
    case Failure(exception) => println(s"the operation was not calculated $exception")
    case Success(value) => println(s"the operation result was $value")
  }

  val aPartialFunction: PartialFunction[Try[Int], Unit] = {
    case Failure(exception) => println(s"the operation was not calculated $exception")
    case Success(value) => println(s"the operation result was $value")
  }

  // map, flatmap, filter
  val doubleAsync: Future[Int] = aFuture.map(_*2)

  //implicits
  // 1 - implicit arguments and values
  // for example the execution context
  implicit val timeout: Int = 300
  def setTimeout(f: ()=> Unit)(implicit tout:Int) = {
    Thread.sleep(tout)
    f()
  }

  setTimeout(()=> println("timeout"))

  // 2 - extension methods
  implicit class MyRichInt(number: Int) {
    def isEven: Boolean = number%2==0
  }

  val is2Even = 2.isEven // scaled compiler wraps this into the myrichint new RichInt(2).isEven

  // 3 - conversion
  // considered dangerous
  implicit def string2Person(name:String):Person =
    new Person(name,57)

  val daniel: Person = "daniel"




  def main(args: Array[String]): Unit = {

  }
}
