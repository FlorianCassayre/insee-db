name := "insee-db"

version := "0.1"

scalaVersion := "2.13.7"

libraryDependencies += "org.apache.commons" % "commons-csv" % "1.9.0"

libraryDependencies += "com.typesafe.akka" %% "akka-http"   % "10.2.7"
libraryDependencies += "com.typesafe.akka" %% "akka-http-spray-json" % "10.2.7"
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.6.17"

libraryDependencies += "args4j" % "args4j" % "2.33"
