package io.github.osmDecorator

import io.github.osmDecorator.Types.{NodeElement, TagElement}

object Types {
  type TagElement = (String, String)
  type NodeElement = (Int, Long)
  type MemberElement = (Long, String, String)
}

case class MetaData(version: Int,
                    timestamp: Long,
                    changeset: Long,
                    uid: Int,
                    user_sid: String)

case class myPoint(latitude: Double,
                 longitude: Double,
                 elevation: Option[Double])

case class NodeMetrics()
//belongToWays: Int
//roadDensity: Double

case class WayMetrics()

case class Node(id: Long,
                metaData: MetaData,
                tags: Array[(String, String)],
                kpis: NodeMetrics,
                point: myPoint)

object Node {
  def apply(node: OsmNode): Node = new Node(node.id,
    new MetaData(node.version, node.timestamp, node.changeset, node.uid, node.user_sid),
    node.tags.map(elem => (elem.key, elem.value)),
    new NodeMetrics(),
    new myPoint(node.latitude, node.longitude, None))
}

case class Way(id: Long,
               metaData: MetaData,
               tags: Array[TagElement],
               kpis: WayMetrics,
               nodes: Array[NodeElement])

/*case class Relation(id: Long,
                    metaData: MetaData,
                    tags: Array[TagElement],
                    members: Array[MemberElement])*/
