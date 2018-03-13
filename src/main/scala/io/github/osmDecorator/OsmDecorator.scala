package io.github.osmDecorator

import java.net.URL
import java.util.UUID

import com.typesafe.config.ConfigFactory
import geotrellis.raster.resample.Bilinear
import geotrellis.spark.TileLayerMetadata
import geotrellis.spark.io.hadoop.Implicits._
import geotrellis.spark.tiling.FloatingLayoutScheme
import geotrellis.spark.util.SparkUtils
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.raster._
import geotrellis.vector._
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object OsmDecorator extends App {

  val home = ConfigFactory.systemEnvironment().getString("HOME")
  val config = ConfigFactory.parseURL(new URL(s"file:///$home/Data/Configurations/OsmDecorator.conf")).resolve()
  System.out.println(config.toString)

  val sc = SparkUtils.createLocalSparkContext("local[*]", "OsmDecorator")
  val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
  val partitioner = new HashPartitioner(100)
  import spark.implicits._

  // Prepare RDD that reads the elevation from NASA files
  val demFolder = new URL(config.getString("dem.tiff.folder.path")).getPath
  val tilesRDD: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD(demFolder)

  //println(tilesRDD.count())

  val (_, rasterMetaData) = TileLayerMetadata.fromRdd(tilesRDD, FloatingLayoutScheme(64))
  val layoutDefinition = rasterMetaData.layout

  val indexedTiles: RDD[(SpatialKey, Feature[Geometry, (ProjectedExtent, Tile)])] = tilesRDD.map { record => Feature(record._1.extent.toPolygon(), record) }.clipToGrid(layoutDefinition)
  val rddPoly/*: RDD[(SpatialKey, Iterable[Feature[Geometry, (ProjectedExtent, Tile)]])]*/ = indexedTiles.groupByKey(partitioner).map(x => (x._1,x._2.size))
  //val magellanTilesRdd = tilesRDD.map(tile=> (Polygon(Array(0),tile._1.extent.toPolygon().vertices.map(point => Point(point.x, point.y))) -> tile)).toDF()
  rddPoly.take(30).foreach(println(_))

  // NODES decoration
  val nodes = spark.read.parquet(config.getString("osm.parquet.nodes.path")).as[OsmNode]
  val indexedNodes: RDD[(SpatialKey,OsmNode)] = nodes.rdd.map(node => (layoutDefinition.mapTransform.pointToKey(node.longitude,node.latitude), node))
  val rddPoints = indexedNodes.groupByKey(partitioner)

  indexedTiles.map{x => println(x._1.extent(layoutDefinition).toString()); x}.take(30).foreach(println(_))
  indexedNodes.take(10).foreach(println(_))
  //val magellanNodes = nodes.rdd.map(node => Point(node.longitude,node.latitude) -> node).toDF()

  //magellanNodes.join(magellanTilesRdd).where($"point" intersects $"polygon").show(25)
  //magellanTilesRdd.join(magellanNodes).where($"point" within $"polygon").show(25)
  //val decoratedNodes = nodes.withColumn("elevation", lit(Int.MaxValue))
  //map(node => Node(node))
  //tmpRdd.take(10).foreach(println(_))
  //decoratedNodes.take(10).foreach(println(_))

  // WAY decoration
  val ways = spark.read.parquet(config.getString("osm.parquet.ways.path")).as[OsmWay]

  // RELATION decoration
  //val relations = spark.read.schema().parquet(config.getString("osm.parquet.relations.path"))
}
