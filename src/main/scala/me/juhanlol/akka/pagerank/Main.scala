package me.juhanlol.akka.pagerank

import com.twitter.cassovary.util.io.ListOfEdgesGraphReader
import com.twitter.cassovary.algorithms.{PageRankParams => CPageRankParams, PageRank => CPageRank}
import java.util.concurrent.Executors
import com.twitter.util.Stopwatch
import me.juhanlol.akka.pagerank.algorithm.{ActorPageRankAlgorithm, PageRankParams, NaivePageRankAlgorithm}
import akka.actor.ActorSystem
import akka.util.Timeout
import scala.concurrent.duration._

// TOO SLOW !
// ListOfEdgeGraphReader is very slow
object CassovaryTest extends App {
  val timer = Stopwatch.start()
  val threadPool = Executors.newFixedThreadPool(1)
  val graph = ListOfEdgesGraphReader.forIntIds(
    "/home/darkjh/projects/scala/akka-pagerank/data",
    "small.txt",
    threadPool
  ).toArrayBasedDirectedGraph()
  val pr = CPageRank(graph, CPageRankParams(0.85, Some(50)))

  println("Used: " + timer().inMilliseconds + "ms")

  val scaleFactor = 1 / pr.sum
  val res = pr.sorted(Ordering[Double].reverse).map(_ * scaleFactor)
  println(pr.sorted(Ordering[Double].reverse) mkString " ")
  println(res mkString " ")

  threadPool.shutdown()
}


object NaivePageRankWithResolution extends App {
  val timer = Stopwatch.start()
  val graph = IntEdgeListFileMappingLoader.loadFromDir(
    "/home/darkjh/projects/scala/akka-pagerank/data",
    "web-Stanford.txt"
//    "small.txt"
  )

  val algo = new NaivePageRankAlgorithm(PageRankParams())
  val pr = algo.execute(graph)
  pr.take(10).foreach(println(_))

  println("Used %d ms".format(timer().inMilliseconds))
}


object ActorPageRankWithResolution extends App {
  val timer = Stopwatch.start()
  val graph = IntEdgeListFileMappingLoader.loadFromDir(
    "/home/darkjh/projects/scala/akka-pagerank/data",
    "web-Stanford.txt"
//    "small.txt"
  )
  implicit val system = ActorSystem("PageRankSystem")
  implicit val timeout = Timeout(100.days)

  val algo = new ActorPageRankAlgorithm(PageRankParams())
  val pr = algo.execute(graph)
  pr.take(10).foreach(println(_))

  println("Used %d ms".format(timer().inMilliseconds))
}