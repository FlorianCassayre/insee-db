name := "insee-db"

version := "0.1"

scalaVersion := "2.13.1"

libraryDependencies += "org.apache.commons" % "commons-csv" % "1.7"

libraryDependencies += "com.typesafe.akka" %% "akka-http"   % "10.1.11"
libraryDependencies += "com.typesafe.akka" %% "akka-http-spray-json" % "10.1.11"
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.5.29"

libraryDependencies += "args4j" % "args4j" % "2.33"
