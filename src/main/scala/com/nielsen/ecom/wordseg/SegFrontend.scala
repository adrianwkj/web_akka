package com.nielsen.ecom.wordseg

import language.postfixOps
import scala.concurrent.duration._
import akka.actor.Inbox
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Terminated
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import java.util.concurrent.atomic.AtomicInteger


//#frontend
class SegFrontend extends Actor {

  var backends = IndexedSeq.empty[ActorRef]
  var jobCounter = 0

  def receive = {
    case job: SegJob if backends.isEmpty =>
      sender() ! JobFailed("Service unavailable, try again later", job)

    case job: SegJob =>
      jobCounter += 1
      backends(jobCounter % backends.size) forward job

    case BackendRegistration if !backends.contains(sender()) =>
      context watch sender()
      backends = backends :+ sender()

    case Terminated(a) =>
      backends = backends.filterNot(_ == a)
      
    case result:SegResult =>
      println(result.meanWordLength)
  }
}
//#frontend

object SegFrontend {
  def main(args: Array[String]): Unit = {
    // Override the configuration of the port when specified as program argument
    val port = if (args.isEmpty) "0" else args(0)
    val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port").
      withFallback(ConfigFactory.parseString("akka.cluster.roles = [frontend]")).
      withFallback(ConfigFactory.load())

    val system = ActorSystem("ClusterSystem", config)
    val frontend = system.actorOf(Props[SegFrontend], name = "frontend")

    val itemdesc = "雀氏柔薄乐动婴儿纸尿裤L168片宝宝大码尿不湿超薄透气银行股票天秤"
    val itemlist = List("雀氏柔薄乐动婴儿纸尿裤L168片宝宝大码","尿不湿超薄透气银行股票天秤")
    
    import system.dispatcher
    
    
    system.scheduler.scheduleOnce(2.seconds) {
      implicit val timeout = Timeout(5 seconds)
      for (i <- itemlist){
      (frontend ? SegJob(i)) onSuccess {
        case result => println(result)
      }
      }
    }

  }
}