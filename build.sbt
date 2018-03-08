name := "osm-decorator"

version := "0.1"

scalaVersion := "2.11.8"

resolvers += "osgeo" at "http://download.osgeo.org/webdav/geotools/"
resolvers += "geosolutions" at "http://maven.geo-solutions.it/"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.1"

libraryDependencies += "org.locationtech.geotrellis" %% "geotrellis-spark"  % "1.2.1"
libraryDependencies += "org.locationtech.geotrellis" %% "geotrellis-geotools" % "1.2.1"

libraryDependencies += "com.typesafe" % "config" % "1.3.3"
