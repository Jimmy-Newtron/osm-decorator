package io.github.osmDecorator

import java.net.URL

import com.typesafe.config.ConfigFactory
import geotrellis.geotools.GridCoverage2DConverters
import geotrellis.raster._
import geotrellis.spark.io.hadoop.Implicits._
import geotrellis.spark.join.SpatialJoin
import geotrellis.spark.partition._
import geotrellis.spark.tiling.FloatingLayoutScheme
import geotrellis.spark.util.SparkUtils
import geotrellis.spark.{Metadata, TileLayerMetadata, _}
import geotrellis.vector._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.geotools.geometry.DirectPosition2D
import org.graphframes.GraphFrame

import scala.util.Try

object OsmDecorator extends App {

  // App Configuration
  val home = ConfigFactory.systemEnvironment().getString("HOME")
  val config = ConfigFactory.parseURL(new URL(s"file:///$home/Data/Configurations/OsmDecorator.conf")).resolve()
  System.out.println(config.toString)
  // Spark context & session
  val sc = SparkUtils.createLocalSparkContext("local[*]", "OsmDecorator")
  val spark = SparkSession.builder().config(sc.getConf).getOrCreate()

  import spark.implicits._

  // Load elevation from NASA files (GeoTiffs)
  val demFolder = new URL(config.getString("dem.tiff.folder.path")).getPath
  val tilesRDD: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD(demFolder)
  // TileLayerMetadata preparation
  val (_, rasterMetaData) = TileLayerMetadata.fromRdd(tilesRDD, FloatingLayoutScheme(64))
  val layoutDefinition = rasterMetaData.layout
  val spacePartitioner = SpacePartitioner(rasterMetaData.bounds)
  // Prepare elevation raster layer
  val indexedTiles: RDD[(SpatialKey, Feature[Geometry, (ProjectedExtent, Tile)])] = tilesRDD.map { record => Feature(record._1.extent.toPolygon(), record) }.clipToGrid(layoutDefinition)
  val tilesLayer: RDD[(SpatialKey, Iterable[Feature[Geometry, (ProjectedExtent, Tile)]])] with Metadata[TileLayerMetadata[SpatialKey]] = ContextRDD(indexedTiles.groupByKey(spacePartitioner), rasterMetaData)

  // NODES decoration
  val nodes: Dataset[OsmNode] = spark.read.parquet(config.getString("osm.parquet.nodes.path")).as[OsmNode]
  // Prepare elevation raster layer
  val indexedNodes: RDD[(SpatialKey, OsmNode)] = nodes.rdd.map(node => (layoutDefinition.mapTransform.pointToKey(node.longitude, node.latitude), node))
  val nodesLayer: RDD[(SpatialKey, Iterable[OsmNode])] with Metadata[TileLayerMetadata[SpatialKey]] = ContextRDD(indexedNodes.groupByKey(spacePartitioner), rasterMetaData)

  val decoratedNodes = SpatialJoin.leftOuterJoin(nodesLayer, tilesLayer).flatMap(addElevation).toDS()
  //decoratedNodes.show(50, false)

  // WAY decoration
  val ways = spark.read.parquet(config.getString("osm.parquet.ways.path")).as[OsmWay]
  ways.map(way => Way(way))
    .filter(_.tags.contains("highway"))
    //.filter(_.tags.contains("building"))
    //.filter(!_.tags.contains("landuse"))
    //.filter(!_.tags.contains("boundary"))
    //.filter(!_.tags.contains("natural"))
    //.filter(!_.tags.contains("waterway"))
    //.filter(!_.tags.contains("railway"))
    //.filter(!_.tags.contains("historic"))
    //.filter(!_.tags.contains("leisure"))
    //.filter(!_.tags.contains("amenity"))
    //.filter(!_.tags.getOrElse("leisure",Set.empty).contains("swimming_pool"))
    //.filter(!_.tags.getOrElse("leisure",Set.empty).contains("pitch"))
    //.filter(!_.tags.getOrElse("amenity",Set.empty).contains("swimming_pool"))
    //.filter(!_.tags.getOrElse("amenity",Set.empty).contains("school"))
    //.filter(!_.tags.getOrElse("amenity",Set.empty).contains("parking"))
    //.filter(_.tags.contains("barrier"))
    //.filter(way => !way.tags.contains("highway") && way.nodes.head.nodeId.equals(way.nodes.last.nodeId))
    .flatMap {
    record => record.tags.toList.map(_ -> 1)
  }.groupByKey(_._1).count().sort($"count(1)".desc)
  //  .show(50, false)

  val edges = ways.map(way => Way(way)).filter(_.tags.contains("highway")).flatMap(splitEdges)
  edges.show(numRows = 50, truncate = false)

  val graph = GraphFrame(decoratedNodes.toDF(), edges.toDF())

  // RELATION decoration
  //val relations = spark.read.schema().parquet(config.getString("osm.parquet.relations.path"))

  // Magellan library
  //val magellanTilesRdd = tilesRDD.map(tile=> (Polygon(Array(0),tile._1.extent.toPolygon().vertices.map(point => Point(point.x, point.y))) -> tile)).toDF()
  //val magellanNodes = nodes.rdd.map(node => Point(node.longitude,node.latitude) -> node).toDF()
  //magellanNodes.join(magellanTilesRdd).where($"point" intersects $"polygon").show(25)
  //magellanTilesRdd.join(magellanNodes).where($"point" within $"polygon").show(25)

  def splitEdges(record: Way): Iterable[Edge] = {
    record.nodes.sliding(2).map(pair => Edge(pair(0).nodeId, pair(1).nodeId, record.id, record.metaData, record.tags)).toIterable
  }

  // TODO: Ensure to get a precise elevation for every point
  def addElevation(record: (SpatialKey, (Iterable[OsmNode], Option[Iterable[Feature[Geometry, (ProjectedExtent, Tile)]]]))): Iterable[Node] = {
    val features = record._2._2.getOrElse(Seq.empty).map(feature => GridCoverage2DConverters.convertToGridCoverage2D(Raster(feature.data._2, feature.data._1.extent)))
    record._2._1.map(
      node => Node(node, average(features.map(
        feature => Try(feature.evaluate(new DirectPosition2D(node.longitude, node.latitude))))
        .filter(_.isSuccess)
        .map(_.get.asInstanceOf[Array[Int]].head)))
    )
  }

  def average[T](ts: Iterable[T])(implicit num: Numeric[T]) = {
    Try(num.toDouble(ts.sum) / ts.size).toOption
  }
}