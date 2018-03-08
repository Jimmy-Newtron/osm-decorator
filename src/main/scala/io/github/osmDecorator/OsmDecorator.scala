package io.github.osmDecorator

import java.io.File
import java.net.URL

import com.typesafe.config.ConfigFactory
import geotrellis.geotools.GridCoverage2DConverters
import geotrellis.raster.{Tile, TileLayout}
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.raster.resample.Bilinear
import geotrellis.spark.io.LayerHeader
import geotrellis.spark.io.hadoop.Implicits._
import geotrellis.spark.partition.Implicits._
import geotrellis.spark.tiling._
import geotrellis.spark.{Bounds, ContextRDD, LayerId, Metadata, SpatialKey, TileLayerMetadata, TileLayerRDD}
import geotrellis.spark.io.file.{FileAttributeStore, FileLayerReader}
import geotrellis.spark.merge.RDDLayoutMerge
import geotrellis.spark.tiling.FloatingLayoutScheme
import geotrellis.spark.util.SparkUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import geotrellis.vector.Point

object OsmDecorator extends App {

  val home = ConfigFactory.systemEnvironment().getString("HOME")
  System.out.println(ConfigFactory.systemEnvironment().toString)
  val config = ConfigFactory.parseURL(new URL(s"file:///$home/Data/Configurations/OsmDecorator.conf")).resolve()
  System.out.println(config.toString)

  val sc = SparkUtils.createLocalSparkContext("local[*]", "OsmDecorator")
  val spark = SparkSession.builder().config(sc.getConf).getOrCreate()

  import spark.implicits._

  val nodes = spark.read.parquet(config.getString("osm.parquet.nodes.path")).as[OsmNode]
  val ways = spark.read.parquet(config.getString("osm.parquet.ways.path")).as[OsmWay]
  //val relations = spark.read.schema().parquet(config.getString("osm.parquet.relations.path"))

  val demFolder = new URL(config.getString("dem.tiff.folder.path")).getPath
  val tilesRDD = sc.hadoopGeoTiffRDD(demFolder)
  val (_, rasterMetaData) = TileLayerMetadata.fromRdd(tilesRDD, FloatingLayoutScheme(512))
  tilesRDD.tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
  println(rasterMetaData.toString)
  //val reader = FileLayerReader(demFolder)(sc)
  //val layerId = LayerId("elevation", 0)
  //val header = reader.attributeStore.readHeader[LayerHeader](layerId)
  //println(header.toString())
  //val rdd = reader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId)
  //rdd.take(50).foreach(println(_))

  //val demTiffs = folder.listFiles().map(file => (file, SinglebandGeoTiff(file.getAbsolutePath)))

  //val covs = demTiffs.map(tiff => GridCoverage2DConverters.convertToGridCoverage2D(tiff._2.raster, tiff._2.crs))

  //val geoTiff = TileStitcher.stitch(geoTiffs.map(record => (record.tile, (0,0))).toIterable, 5, 5)
  //val slope = Slope(geoTiff,Square(1),None,new CellSize(5,5),8)
  /*println(geoTiff.extent.toString())
  println(geoTiff.tags.toString)
  println(geoTiff.options.toString)
  println(geoTiff.tile.asciiDraw())*/

  // NODES decoration
  //covs.map(cov => cov.evaluate(new DirectPosition2D(cov.getCoordinateReferenceSystem, , )))
  val decoratedNodes = nodes.rdd.map(node => (Point(node.longitude,node.latitude), node))
  //val decoratedNodes = nodes.withColumn("elevation", lit(Int.MaxValue))
  //map(node => Node(node))
  decoratedNodes.take(10).foreach(println(_))

  tilesRDD
  //relations.take(10).foreach(println(_))

  // WAY decoration
  //val decoratedWays = ways.map(way => )

  //filter(_.tags.toMap[String, String].contains("highway"))

  //decoratedWays.take(50).foreach(println(_))
}
