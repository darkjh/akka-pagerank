package me.juhanlol.akka.pagerank.algorithm

import me.juhanlol.akka.pagerank.Graph
import akka.actor._


sealed trait PRMessage

case object Compute extends PRMessage

case class Work(modulo: Int) extends PRMessage

case class Result(value: Double) extends PRMessage


class ActorPageRankAlgorithm(params: PageRankParams)(implicit system: ActorSystem)
  extends PageRankAlgorithm {
  override def execute(graph: Graph): PageRank = ???
}