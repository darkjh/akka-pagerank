package me.juhanlol.akka.pagerank

import com.google.common.collect.BiMap


case class AdjacencyList(source: Int, outDegree: Int, dests: Array[Int])

case class Graph(
  sources: Array[AdjacencyList],
  maxId: Int, nodeCount: Int,
  mapping: NodeIdMapping
)

trait NodeIdMapping {
  def getInternalId(externalId: Int): Int
  def getExternalId(internalId: Int): Int
}

case class BiMapNodeIdMapping(conversionTable: BiMap[Int, Int])
  extends NodeIdMapping {
  val intToExt = conversionTable
  val extToInt = conversionTable.inverse()

  def getInternalId(externalId: Int): Int = extToInt.get(externalId)
  def getExternalId(internalId: Int): Int = intToExt.get(internalId)
}