package init

import akka.camel.{ CamelMessage, Consumer }

/**
 * File endpoint
 */
class FileEndpoint extends Consumer {
  def endpointUri = "file://C:/tmp/camel"

  def receive = {
    case msg: CamelMessage => { println("received %s" format msg.bodyAs[String]) } 
  }
}

/**
 * Jetty endpoint
 */
class JettyEndpoint extends Consumer {
  def endpointUri = "jetty:http://127.0.0.1:6277/camel/default"

  def receive = {
    case msg: CamelMessage => sender() ! ("Hello %s" format msg.bodyAs[String])
  }
}