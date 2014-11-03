package me.juhanlol.akka.pagerank

import com.google.common.collect.BiMap


case class AdjacencyList(source: Int, outDegree: Int, dests: Array[Int])

case class Graph(
  sources: Array[AdjacencyList],
  maxId: Int, nodeCount: Int,
  mapping: Option[NodeIdMapping] = None
)

case class NodeIdMapping(conversionTable: BiMap[Int, Int]) {
  val intToExt = conversionTable
  val extToInt = conversionTable.inverse()

  def getInternalId(externalId: Int): Int = extToInt.get(externalId)
  def getExternalId(internalId: Int): Int = intToExt.get(internalId)
}