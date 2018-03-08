package io.github.osmDecorator

case class OsmTagElement(key: String, value: String)

case class OsmNodeElement(index: Int, nodeId: Long)

case class OsmMemberElement(id: Long, role: String, memberType: String)

case class OsmNode(id: Long,
                version: Int,
                timestamp: Long,
                changeset: Long,
                uid: Int,
                user_sid: String,
                tags: Array[OsmTagElement],
                latitude: Double,
                longitude: Double)

case class OsmWay(id: Long,
               version: Int,
               timestamp: Long,
               changeset: Long,
               uid: Int,
               user_sid: String,
               tags: Array[OsmTagElement],
               nodes: Array[OsmNodeElement])

case class OsmRelation(id: Long,
                    version: Int,
                    timestamp: Long,
                    changeset: Long,
                    uid: Int,
                    user_sid: String,
                    tags: Array[OsmTagElement],
                    members: Array[OsmMemberElement])