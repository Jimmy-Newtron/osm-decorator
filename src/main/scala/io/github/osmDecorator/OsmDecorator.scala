package io.github.osmDecorator

import java.io.File
import java.net.URL
import java.nio.file.{Files, Paths}

import com.typesafe.config.ConfigFactory
import geotrellis.raster._
import geotrellis.spark.io.hadoop.Implicits._
import geotrellis.spark.join.SpatialJoin
import geotrellis.spark.partition._
import geotrellis.spark.tiling.FloatingLayoutScheme
import geotrellis.spark.util.SparkUtils
import geotrellis.spark.{Metadata, TileLayerMetadata, _}
import geotrellis.vector._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.graphframes.GraphFrame
import org.neo4j.spark.Neo4j

import scala.io.Source
import scala.util.Try

object OsmDecorator extends App {

  private val forcedExecution = Try(config.getBoolean("override")).getOrElse(false)

  // App Configuration
  val home = ConfigFactory.systemEnvironment().getString("HOME")
  val config = ConfigFactory.parseURL(new URL(s"file:///$home/Data/Configurations/OsmDecorator.conf")).resolve()
  // System.out.println(config.toString)
  val summaryUrl = new URL(config.getString("orc.processes.summary"))
  var processes: Set[String] = if (Files.exists(Paths.get(summaryUrl.getPath))) {
    Source.fromURL(summaryUrl).getLines().toSet
  } else {
    Paths.get(summaryUrl.getPath).getParent.toFile.mkdirs()
    Set.empty
  }

  // Spark context & session
  val sparkConf = new SparkConf().set("spark.neo4j.bolt.password", "neo4j")
  val sc = SparkUtils.createLocalSparkContext("local[*]", "OsmDecorator", sparkConf)
  val neo = Neo4j(sc)
  val spark = SparkSession.builder().config(sc.getConf).getOrCreate()

  import spark.implicits._

  Functions.executeProcess("init graph", processes, true) { () =>
    if (true || neo.nodes.isEmpty) {
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

      val decoratedNodes = SpatialJoin.leftOuterJoin(nodesLayer, tilesLayer).flatMap(Functions.addElevation).toDS

      // WAY decoration
      val ways = spark.read.parquet(config.getString("osm.parquet.ways.path")).as[OsmWay]
      val edges = ways.map(way => Way(way)).filter(way => way.tags.contains("highway") && way.nodes.length > 1).flatMap(Functions.splitEdges)

      val graph = GraphFrame(decoratedNodes.toDF, edges.toDF)
      neo.saveGraph(graph.toGraphX, null, neo.pattern, true)
    }
  }

  Functions.executeProcess("show graph", processes, true) { () =>
    val graph = neo.loadGraphFrame
    graph.vertices.show(50, false)
    graph.edges.show(50, false)
  }

  processes = Functions.executeProcess("update vertex: degree", processes, true) { () =>
    val graph = neo.loadGraphFrame
    val v = graph.vertices
    val d = graph.degrees

    //d.join(v, "id").withColumnRenamed("degree", "kpis.degree").drop("degree").write.mode(SaveMode.Overwrite).parquet(verticesPath)
  }

  processes = Functions.executeProcess("update edge: slope", processes, false) { () =>
    val graph = neo.loadGraphFrame
    graph.triplets.schema.printTreeString()
    //graph.triplets.show(50, false)
  }

  Functions.printToFile(new File(summaryUrl.getPath)) { p =>
    processes.filterNot(_.isEmpty).foreach(p.println)
  }
}