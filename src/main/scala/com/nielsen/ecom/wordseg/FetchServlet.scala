package com.nielsen.ecom.wordseg;

import akka.actor._
import akka.routing.RoundRobinRouter
import scala.concurrent.duration._
import scalaj.http.Http
import scala.io._
import scalax.io._
import java.io._

object FetchServlet extends App {
 
  val RawData = Source.fromFile("rawdata_GBK.txt").getLines().toList
  
  calculate(nrOfWorkers = 500, RawData)
 
  sealed trait PiMessage
  case object Calculate extends PiMessage
  case class Work(Brand: String,ItemDesc: String, ModelName: String) extends PiMessage
  case class Result(value: String) extends PiMessage
  case class PiApproximation(duration: Duration)
 
  class Worker extends Actor {
 
    def CallServlet(Brand: String,ItemDesc: String, ModelName: String): String = {
      val url = "http://10.250.33.105:80/nielsen_web2/json"
      val holder = Http(url)
      val result = holder.params(Map("brand" -> Brand,
        "longdesc" -> ItemDesc, "model" -> ModelName,
        "col10" -> "1","col11" -> "2","col12" -> "3",
        "col13" -> "4","col14" -> "5")).asString
      Brand + "," + ItemDesc + "," + result.body
    }
 
    def receive = {
      case Work(a, b, c) =>
        sender ! Result(CallServlet(a, b, c)) // perform the work
    }
  }
 
  class Master(nrOfWorkers: Int, RawData: List[String], listener: ActorRef)
    extends Actor {
 
    var codingResults: List[String] = _
    var nrOfResults: Int = _
    val start: Long = System.currentTimeMillis
    val fw = new FileWriter("result.txt", true)
 
    val workerRouter = context.actorOf(
      Props[Worker].withRouter(RoundRobinRouter(nrOfWorkers)), name = "workerRouter")
 
    def receive = {
      case Calculate =>
        for (i <- RawData) {
          val Item = i.split(",")
          val Brand = Item(9)
          val ItemDesc = Item(10) + Item(11)
          val ModelName = Item(4)
          workerRouter ! Work(Brand, ItemDesc, ModelName)
        }
      case Result(value) =>
        //println(value)
        fw.write(value + "\n")
        nrOfResults += 1
        if (nrOfResults == RawData.size) {
          fw.close()
          // Send the result to the listener
          listener ! PiApproximation(duration = (System.currentTimeMillis - start).millis)
          // Stops this actor and all its supervised children
          context.stop(self)
        }
    }
 
  }
 
  class Listener extends Actor {
    def receive = {
      case PiApproximation(duration) =>
        println("\n\tCalculation time: \t\n%s"
          .format(duration))
        context.system.shutdown()
    }
  }
 
 
  def calculate(nrOfWorkers: Int, RawData: List[String]) {
    // Create an Akka system
    val system = ActorSystem("PiSystem")
 
    // create the result listener, which will print the result and shutdown the system
    val listener = system.actorOf(Props[Listener], name = "listener")
 
    // create the master
    val master = system.actorOf(Props(new Master(
      nrOfWorkers, RawData, listener)),
      name = "master")
 
    // start the calculation
    master ! Calculate
 
  }
}