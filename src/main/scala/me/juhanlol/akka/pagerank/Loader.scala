package me.juhanlol.akka.pagerank

import java.io.{IOException, File}
import scala.io.Source
import it.unimi.dsi.fastutil.ints.{IntLinkedOpenHashSet, Int2ObjectLinkedOpenHashMap}
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable
import com.google.common.collect.HashBiMap
import scala.collection.JavaConversions._


trait GraphLoader[T] {
  def readFile(directory: String, prefix: String = ""): Iterator[String] = {
    val dir = new File(directory)
    val filesInDir = dir.list()
    if (filesInDir == null) {
      throw new IOException("Current directory is "
        + System.getProperty("user.dir")
        + " and nothing was found in dir " + dir)
    }
    val validFiles = filesInDir.flatMap {
      filename =>
        if (filename.startsWith(prefix)) {
          Some(filename)
        }
        else {
          None
        }
    }
    validFiles.map {
      filename =>
        Source.fromFile(directory + "/" + filename)
          .getLines()
          .filterNot {
            case GraphLoader.commentPattern(s) => true
            case _ => false
          }
    }.foldLeft(Iterator.empty: Iterator[String])(_ ++ _)
  }

  def readId(str: String): T

  def cast(line: String): mutable.ArraySeq[T] =
    line.split("\\s").map(readId)
}
object GraphLoader {
  val commentPattern = """(^#.*)""".r
}


trait IntGraphLoader extends GraphLoader[Int] {
  override def readId(str: String) = str.toInt
}

// Edge list loader with node id resolution
object IntEdgeListFileMappingLoader extends IntGraphLoader {
  def loadFromDir(directory: String, prefix: String = ""): Graph = {
    var iter = readFile(directory, prefix).map(cast)
    val edgesBySource = new Int2ObjectLinkedOpenHashMap[ArrayBuffer[Int]]()
    val allNodes = mutable.SortedSet[Int]()

    // first pass
    // collect all nodes
    iter.foreach { array =>
      allNodes.add(array(0))
      allNodes.add(array(1))
    }

    // construct id mapping
    var internalId = 0
    val mapping = HashBiMap.create[Int, Int]()
    for (i <- allNodes) {
      mapping.put(internalId, i)
      internalId += 1
    }
    val nodeIdMapping = BiMapNodeIdMapping(mapping)

    // second pass
    // construct adjacency lists while translating the node ids
    iter = readFile(directory, prefix).map(cast)
    iter.foreach { array =>
      val s = nodeIdMapping.getInternalId(array(0))
      val d = nodeIdMapping.getInternalId(array(1))

      if (edgesBySource.containsKey(s)) {
        edgesBySource.get(s) += d
      } else {
        edgesBySource.put(s, ArrayBuffer(d))
      }
    }

    val maxId = allNodes.last
    val nodeCount = allNodes.size

    // construct the graph
    val sources = new Array[AdjacencyList](nodeCount)
    for (i <- 0 until nodeCount) {
      sources(i) = edgesBySource.get(i) match {
        case null => null // dead end node
        case l => AdjacencyList(i, l.size, l.toArray) // normal node
      }
    }

    Graph(sources, maxId, nodeCount, nodeIdMapping)
  }
}