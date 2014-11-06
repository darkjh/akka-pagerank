package me.juhanlol.akka.pagerank.algorithm

import me.juhanlol.akka.pagerank.Graph
import com.twitter.logging.Logger
import com.twitter.util.Stopwatch
import breeze.linalg._


class NaivePageRankAlgorithm(params: PageRankParams)
  extends PageRankAlgorithm {

  private val log = Logger.get(getClass)

  override def execute(graph: Graph): PageRank = {
    val timer = Stopwatch.start()

    // page rank constants
    val dampeningFactor = params.dampeningFactor
    val iterations = params.iterations.getOrElse(Int.MaxValue)
    val gamma = params.gamma

    val nodeCount = graph.nodeCount

    // main loop
    var residual = Double.MaxValue
    var src = DenseVector.fill(nodeCount, 1.0 / nodeCount)

    var iterCount = 0
    while (residual > gamma && iterCount < iterations) {
      log.info("Start iteration: %d ...\n", iterCount)

      val dst = DenseVector.zeros[Double](nodeCount)

      for (s <- graph.sources if s != null) {
        val weight = src(s.source) / s.outDegree
        for (j <- s.dests) {
          dst(j) = dst(j) + weight
        }
      }

      dst :*= dampeningFactor
//      // if without dead-ends
//      dst :+= (1 - dampeningFactor) / nodeCount
      // with dead-ends
      dst :+= (1 - sum(dst)) / nodeCount

      residual = norm(src - dst)
      log.info(residual.toString)
      src = dst
      iterCount += 1
    }

    log.info("Used: %d ms, after %d iterations\n",
      timer().inMilliseconds, iterCount)

    new PageRank(src, 1.0, graph.mapping)
   }
}