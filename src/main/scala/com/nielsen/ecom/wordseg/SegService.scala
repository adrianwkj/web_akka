package com.nielsen.ecom.wordseg

import scala.concurrent.duration._
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.ReceiveTimeout
import akka.routing.ConsistentHashingRouter.ConsistentHashableEnvelope
import akka.routing.FromConfig

//#service
class SegService extends Actor {
  // This router is used both with lookup and deploy of routees. If you
  // have a router with only lookup of routees you can use Props.empty
  // instead of Props[StatsWorker.class].
  val workerRouter = context.actorOf(FromConfig.props(Props[SegWorker]),
    name = "workerRouter")

  def receive = {
    case SegJob(text) if text != "" =>
      val words = text.split(" ")
      val replyTo = sender() // important to not close over sender()
      // create actor that collects replies from workers
      val aggregator = context.actorOf(Props(
        classOf[StatsAggregator], words.size, replyTo))
      words foreach { word =>
        workerRouter.tell(
          ConsistentHashableEnvelope(word, word), aggregator)
      }
  }
}

class StatsAggregator(expectedResults: Int, replyTo: ActorRef) extends Actor {
  var results = IndexedSeq.empty[Int]
  context.setReceiveTimeout(3.seconds)

  def receive = {
    case wordCount: String =>
      //results = results :+ wordCount

      val meanWordLength = 123456
      replyTo ! SegResult(wordCount)
      context.stop(self)

    case ReceiveTimeout =>
      replyTo ! JobFailed("Service unavailable, try again later",SegJob(""))
      context.stop(self)
  }
}
//#service
