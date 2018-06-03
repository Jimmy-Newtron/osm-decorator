name := "osm-decorator"

version := "0.1"

scalaVersion := "2.11.8"

// Config Typesafe
libraryDependencies += "com.typesafe" % "config" % "1.3.3"

// Spark
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.3.0"
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

// Neo4j-Spark
resolvers += Resolver.mavenLocal
libraryDependencies += "neo4j-contrib" % "neo4j-spark-connector" % "2.1.0-M5"

// GraphFrames
libraryDependencies += "default" % "graphframes_2.11" % "0.6.0-SNAPSHOT-spark2.3"
//libraryDependencies += "graphframes" % "graphframes" % "0.5.0-spark2.1-s_2.11"

// Magellan
libraryDependencies += "harsha2010" % "magellan" % "1.0.5-s_2.11"

// Geotrellis + geotools
resolvers += "osgeo" at "http://download.osgeo.org/webdav/geotools/"
resolvers += "geosolutions" at "http://maven.geo-solutions.it/"
libraryDependencies += "org.locationtech.geotrellis" %% "geotrellis-spark"  % "1.2.1"
libraryDependencies += "org.locationtech.geotrellis" %% "geotrellis-geotools" % "1.2.1"