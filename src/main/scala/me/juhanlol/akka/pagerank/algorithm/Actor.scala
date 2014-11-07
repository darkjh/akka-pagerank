package me.juhanlol.akka.pagerank.algorithm

import me.juhanlol.akka.pagerank.Graph
import akka.actor._
import breeze.linalg._
import akka.routing.RoundRobinRouter


sealed trait PRMessage

case object Compute extends PRMessage

case class Work(start: Int, size: Int, graph: Graph) extends PRMessage

case class PartialResult(start: Int, r: DenseVector[Double]) extends PRMessage


class Worker extends Actor {
  def partialUpdate() = {

  }

  override def receive: Actor.Receive = {
    case Work(start, size, g) =>
  }
}

class Master(g: Graph, partitions: Int) extends Actor {
  val workerRouter = context.actorOf(
      Props[Worker].withRouter(RoundRobinRouter(partitions)),
    name = "workerRouter")

  val size = g.nodeCount / partitions
  var starts = 0 until g.nodeCount by size

  override def receive: Actor.Receive = {
    case Compute =>
      for (start <- starts) {
        val s = if (g.nodeCount - start < size) {
          // last one
          g.nodeCount - start
        } else {
          size
        }
        workerRouter ! Work(start, size, g)
      }
  }
}


class ActorPageRankAlgorithm(params: PageRankParams)(implicit system: ActorSystem)
  extends PageRankAlgorithm {
  override def execute(graph: Graph): PageRank = ???
}