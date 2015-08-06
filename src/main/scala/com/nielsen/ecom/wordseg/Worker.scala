package com.nielsen.ecom.wordseg

import akka.actor.Actor
import org.ansj.splitWord.analysis._

//#worker
class SegWorker extends Actor {
  def receive = {
    case word: String =>
      val result = ToAnalysis.parse(word).toString()
      sender() ! result
  }
}