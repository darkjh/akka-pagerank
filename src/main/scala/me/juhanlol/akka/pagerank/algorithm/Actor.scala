package me.juhanlol.akka.pagerank.algorithm

import me.juhanlol.akka.pagerank.Graph
import akka.actor._
import breeze.linalg._
import akka.routing.RoundRobinRouter
import scala.collection.mutable.ArrayBuffer


sealed trait PRMessage

case object Compute extends PRMessage

case class Work(start: Int, size: Int,
                graph: Graph, src: DenseVector[Double])
  extends PRMessage

case class PartialResult(start: Int, r: Array[Double]) extends PRMessage

case class PageRankResult(pr: DenseVector[Double]) extends PRMessage

case object GetResult extends PRMessage


class Worker extends Actor {
  def partialUpdate(start: Int, size: Int, g: Graph,
                    src: DenseVector[Double]): Array[Double] = {
    val res = Array[Double](size)
    for (s <- g.sources if s != null) {
      val weight = src(s.source) / s.outDegree
      for (j <- s.dests) {
        if (j >= start && j < start + size) {
          val idx = j % start
          res(idx) = res(idx) + weight
        }
      }
    }
    res
  }

  override def receive: Actor.Receive = {
    case Work(start, size, g, src) =>
      val res = partialUpdate(start, size, g, src)
      sender ! PartialResult(start, res)
  }
}

class Master(g: Graph, partitions: Int,
             params: PageRankParams, Listener: ActorRef)
  extends Actor {
  val workerRouter = context.actorOf(
      Props[Worker].withRouter(RoundRobinRouter(partitions)),
    name = "workerRouter")

  val size = g.nodeCount / partitions
  var starts = 0 until g.nodeCount by size

  var src = DenseVector.fill(g.nodeCount, 1.0 / g.nodeCount)
  val partialResults = ArrayBuffer[(Int, Array[Double])]()

  override def receive: Actor.Receive = {
    case Compute =>
      for (start <- starts) {
        val s = if (g.nodeCount - start < size) {
          // last one
          g.nodeCount - start
        } else {
          size
        }
        workerRouter ! Work(start, s, g, src)
      }
    case PartialResult(start, res) =>
      partialResults.append((start, res))
      if (partialResults.length == partitions) {
        // all received
        val res = partialResults.sortBy(_._1).map(_._2).reduceLeft(_ ++ _)
        val dst = DenseVector(res)

        dst :*=  params.dampeningFactor
        dst :+= (1 - sum(dst)) / g.nodeCount

        val residual = norm(src - dst)
        src = dst

        if (residual > params.gamma) {
          self ! Compute
        } else {
          // all done
          context.stop(self)
        }
      }
  }
}

class ResultListener extends Actor {
  def receive = {
    case PageRankResult(pr) =>
      pr.slice(0, 10).foreach(println(_))
      context.system.shutdown()
  }
}

class ActorPageRankAlgorithm(params: PageRankParams)(implicit system: ActorSystem)
  extends PageRankAlgorithm {
  override def execute(graph: Graph): PageRank = {
    val listener = system.actorOf(Props[ResultListener], name = "listener")
    val master = system.actorOf(Props[Master](new Master(
      graph, 3, params, listener)), name = "Master")

    master ! Compute

    listener
  }
}