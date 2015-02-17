import akka.pattern.ask
import akka.actor.{ ActorRef, ActorSystem, Props, Actor, Inbox }
import akka.util.Timeout
import akka.camel.CamelMessage
import scala.concurrent.duration._
import main._


case object Greet
case class WhoToGreet(who: String)
case class Greeting(message: String)

class Greeter extends Actor {
  var greeting = ""

  def receive = {
    case WhoToGreet(who) => greeting = s"hello, $who"
    case Greet => sender ! Greeting(greeting) // Send the current greeting back to the sender
  }
}

/*
object starter extends App {
  implicit val timeout = Timeout(10 seconds)
  
  val system = ActorSystem("some-system")
  
  // File endpont
  val file = system.actorOf(Props[FileEndpoint])
  
  // Jetty endpoint
  val jetty = system.actorOf(Props[JettyEndpoint])
  
  // Camel producer
  // val producer = system.actorOf(Props[Producer1])
  // val future = producer.ask("some request").mapTo[CamelMessage]
}
*/

/*
object HelloAkkaScala extends App {

  // Create the 'helloakka' actor system
  val system = ActorSystem("helloakka")

  // Create the 'greeter' actor
  val greeter = system.actorOf(Props[Greeter], "greeter")

  // Create an "actor-in-a-box"
  val inbox = Inbox.create(system)

  // Tell the 'greeter' to change its 'greeting' message
  greeter.tell(WhoToGreet("akka"), ActorRef.noSender)
  // greeter ! WhoToGreet("akka")

  // Ask the 'greeter for the latest 'greeting'
  // Reply should go to the "actor-in-a-box"
  inbox.send(greeter, Greet)

  // Wait 5 seconds for the reply with the 'greeting' message
  val Greeting(message1) = inbox.receive(5.seconds)
  println(s"Greeting: $message1")

  // Change the greeting and ask for it again
  greeter.tell(WhoToGreet("typesafe"), ActorRef.noSender)
  inbox.send(greeter, Greet)
  val Greeting(message2) = inbox.receive(5.seconds)
  println(s"Greeting: $message2")

  val greetPrinter = system.actorOf(Props[GreetPrinter])
  // after zero seconds, send a Greet message every second to the greeter with a sender of the greetPrinter
  system.scheduler.schedule(0.seconds, 1.second, greeter, Greet)(system.dispatcher, greetPrinter)
  
}
*/

// prints a greeting
class GreetPrinter extends Actor {
  def receive = {
    case Greeting(message) => println(message)
  }
}