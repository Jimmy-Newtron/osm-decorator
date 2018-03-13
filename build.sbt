name := "osm-decorator"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.1"

resolvers += "osgeo" at "http://download.osgeo.org/webdav/geotools/"
resolvers += "geosolutions" at "http://maven.geo-solutions.it/"
libraryDependencies += "org.locationtech.geotrellis" %% "geotrellis-spark"  % "1.2.1"
libraryDependencies += "org.locationtech.geotrellis" %% "geotrellis-geotools" % "1.2.1"

resolvers += "spark-packages" at "http://dl.bintray.com/spark-packages/maven"
libraryDependencies += "harsha2010" % "magellan" % "1.0.5-s_2.11"

libraryDependencies += "com.typesafe" % "config" % "1.3.3"
