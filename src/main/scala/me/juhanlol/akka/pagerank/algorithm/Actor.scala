package me.juhanlol.akka.pagerank.algorithm

import me.juhanlol.akka.pagerank.Graph
import akka.actor._
import breeze.linalg._
import akka.routing.RoundRobinRouter
import akka.pattern.ask
import scala.collection.mutable.ArrayBuffer
import com.twitter.logging.Logger
import scala.concurrent.Await
import scala.concurrent.duration._
import akka.util.Timeout


sealed trait PRMessage

case object Compute extends PRMessage

case object Iteration extends PRMessage

case class Work(start: Int, size: Int,
                graph: Graph, src: DenseVector[Double])
  extends PRMessage

case class PartialResult(start: Int, r: Array[Double]) extends PRMessage


class Worker extends Actor {
  def partialUpdate(start: Int, size: Int, g: Graph,
                    src: DenseVector[Double]): Array[Double] = {
    val res = new Array[Double](size)

    for (s <- g.sources if s != null) {
      val weight = src(s.source) / s.outDegree
      for (j <- s.dests) {
        if (j >= start && j < start + size) {
          val idx = j % size
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

class Master(g: Graph, nbPartitions: Int, params: PageRankParams)
  extends Actor {
  private val logger = Logger.get(getClass)
  val workerRouter = context.actorOf(
      Props[Worker].withRouter(RoundRobinRouter(nbPartitions)),
    name = "workerRouter")

  val partitions = partition(g.nodeCount, nbPartitions)

  var src = DenseVector.fill(g.nodeCount, 1.0 / g.nodeCount)
  val partialResults = ArrayBuffer[(Int, Array[Double])]()

  var replyTo: ActorRef = _

  def partition(max: Int, nbPartitions: Int) = {
    val size = max / nbPartitions
    val partitions = (0 until max by size).zipWithIndex.map {
      p => (p._2, p._1)
    }

    if (partitions.size > nbPartitions) {
      partitions.dropRight(1).map {
        case (i, s) if i == (nbPartitions-1) =>
          (s, max - s)
        case (i, s) => (s, size)
      }
    } else {
      partitions.map {
        case (i, s) => (s, size)
      }
    }
  }

  override def receive: Actor.Receive = {
    case Compute =>
      logger.info("Received compute command ...")
      replyTo = sender()
      self ! Iteration

    case Iteration =>
      logger.info("Start a new iteration ...")
      for ((start, size) <- partitions) {
        workerRouter ! Work(start, size, g, src)
      }
    case PartialResult(start, res) =>
      partialResults.append((start, res))
      if (partialResults.length == nbPartitions) {
        // all received
        val res = partialResults.sortBy(_._1).map(_._2).reduceLeft(_ ++ _)
        val dst = DenseVector(res)

        dst :*=  params.dampeningFactor
        dst :+= (1 - sum(dst)) / g.nodeCount

        val residual = norm(src - dst)
        src = dst

        partialResults.clear()
        if (residual > params.gamma) {
          self ! Iteration
        } else {
          // all done
          // send final result
          replyTo ! src
          // stop itself
          context.system.shutdown()
        }
      }
  }
}

class ActorPageRankAlgorithm(params: PageRankParams)
                            (implicit system: ActorSystem,
                             timeout: Timeout)
  extends PageRankAlgorithm {
  override def execute(graph: Graph): PageRank = {
    val master = system.actorOf(Props[Master](new Master(
      graph, 8, params)), name = "Master")

    val pr = (master ? Compute).mapTo[DenseVector[Double]]
    val result = Await.result(pr, Duration.Inf)

    new PageRank(result, 1.0, graph.mapping)
  }
}