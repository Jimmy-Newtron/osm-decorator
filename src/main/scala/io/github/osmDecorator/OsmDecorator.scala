package io.github.osmDecorator

import java.net.URL

import com.typesafe.config.ConfigFactory
import geotrellis.raster._
import geotrellis.spark.{Metadata, TileLayerMetadata, _}
import geotrellis.spark.io.hadoop.Implicits._
import geotrellis.spark.join.SpatialJoin
import geotrellis.spark.partition._
import geotrellis.spark.tiling.FloatingLayoutScheme
import geotrellis.spark.util.SparkUtils
import geotrellis.vector._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

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
  tilesLayer.map(record => record._1 -> record._2.reduce(_))

  // NODES decoration
  val nodes: Dataset[OsmNode] = spark.read.parquet(config.getString("osm.parquet.nodes.path")).as[OsmNode]
  // Prepare elevation raster layer
  val indexedNodes: RDD[(SpatialKey, OsmNode)] = nodes.rdd.map(node => (layoutDefinition.mapTransform.pointToKey(node.longitude, node.latitude), node))
  val nodesLayer: RDD[(SpatialKey, Iterable[OsmNode])] with Metadata[TileLayerMetadata[SpatialKey]] = ContextRDD(indexedNodes.groupByKey(spacePartitioner), rasterMetaData)

  val joinRdd = SpatialJoin.leftOuterJoin(tilesLayer, nodesLayer).filter(_._2._2.isDefined)
  joinRdd.take(30).foreach(println(_))

  //val decoratedNodes = nodes.withColumn("elevation", lit(Int.MaxValue))
  //decoratedNodes.take(50).foreach(println(_))

  // WAY decoration
  val ways = spark.read.parquet(config.getString("osm.parquet.ways.path")).as[OsmWay]

  // RELATION decoration
  //val relations = spark.read.schema().parquet(config.getString("osm.parquet.relations.path"))

  // Magellan library
  //val magellanTilesRdd = tilesRDD.map(tile=> (Polygon(Array(0),tile._1.extent.toPolygon().vertices.map(point => Point(point.x, point.y))) -> tile)).toDF()
  //val magellanNodes = nodes.rdd.map(node => Point(node.longitude,node.latitude) -> node).toDF()
  //magellanNodes.join(magellanTilesRdd).where($"point" intersects $"polygon").show(25)
  //magellanTilesRdd.join(magellanNodes).where($"point" within $"polygon").show(25)
}
