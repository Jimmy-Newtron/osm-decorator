package io.github.osmDecorator

import magellan.Point

case class MetaData(version: Int, timestamp: Long, changeset: Long, uid: Int, user_sid: String)

case class NodeMetrics(elevation: Option[Double], degree: Option[Integer])

case class Node(id: Long,
                metaData: MetaData,
                tags: Map[String, Set[String]],
                kpis: NodeMetrics,
                point: Point)

object Node {
  def apply(node: OsmNode): Node = Node(node, None)

  def apply(node: OsmNode, elevation: Option[Double]): Node = Node(node.id,
    MetaData(node.version, node.timestamp, node.changeset, node.uid, node.user_sid),
    node.tags.groupBy(_.key).map(record => record._1 -> record._2.map(_.value).toSet),
    NodeMetrics(elevation, None),
    Point(node.longitude, node.latitude))
}


/** *****************************/

//belongToWays: Int
//roadDensity: Double

case class WayMetrics()

case class Way(id: Long,
               metaData: MetaData,
               tags: Map[String, Set[String]],
               //kpis: WayMetrics,
               nodes: Array[OsmNodeElement])

object Way {
  def apply(way: OsmWay): Way = Way(way.id,
    MetaData(way.version, way.timestamp, way.changeset, way.uid, way.user_sid),
    way.tags.groupBy(_.key).map(record => record._1 -> record._2.map(_.value).toSet),
    //WayMetrics(),
    way.nodes) // TODO: build the line polygon
}

/** *****************************/

case class Edge(src: Long,
                dst: Long,
                id: Long,
                metaData: MetaData,
                tags: Map[String, Set[String]])
