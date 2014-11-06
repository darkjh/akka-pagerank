package me.juhanlol.akka.pagerank.algorithm

import breeze.linalg._
import me.juhanlol.akka.pagerank.{Graph, NodeIdMapping}


case class PageRankParams(dampeningFactor: Double = 0.85,
                          gamma: Double = 1e-5,
                          iterations: Option[Int] = Some(100))

class PageRank(pr: DenseVector[Double],
               scale: Double,
               idMapping: NodeIdMapping) extends Iterable[(Int, Double)] {
  // normalize to pre-defined scale
  pr :*= (scale / pr.sum)

  def apply(id: Int) = pr(idMapping.getInternalId(id))

  override def iterator: Iterator[(Int, Double)] =
    pr.iterator.map {
      case (intId, v) => (idMapping.getExternalId(intId), v)
    }
}

trait PageRankAlgorithm {
  def execute(graph: Graph): PageRank
}