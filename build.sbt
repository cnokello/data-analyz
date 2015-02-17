name := """dl-reporting"""

version := "1.0" 

scalaVersion := "2.11.1"

assemblyJarName in assembly := "dl-analytics.jar"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % "2.10.4",
  "com.typesafe.akka" %% "akka-actor" % "2.3.7",
  "com.typesafe.akka" %% "akka-testkit" % "2.3.7",
  "com.typesafe.akka" %% "akka-slf4j" % "2.3.2" % "compile",
  "com.typesafe.akka" %% "akka-remote" % "2.3.7",
  "com.typesafe.akka" %% "akka-camel" % "2.3.7"
  		exclude("org.slf4j", "slf4j-api"),
  "org.apache.camel" % "camel-core" % "2.14.0"
  		exclude("org.slf4j", "slf4j-api"),
  "org.apache.camel" % "camel-jetty" % "2.14.0"
  		exclude("org.slf4j", "slf4j-api"),
  "org.apache.camel" % "camel-quartz" % "2.14.0"
  		exclude("org.slf4j", "slf4j-api"),
  "ch.qos.logback" % "logback-classic" % "1.0.7",
  "org.scalatest" %% "scalatest" % "2.1.6" % "test"
  		exclude("org.mockito", "mockito-core"),
  "junit" % "junit" % "4.11" % "test"
  		exclude("org.mockito", "mockito-core"),
  "com.novocode" % "junit-interface" % "0.10" % "test",
   "org.postgresql" % "postgresql" % "9.3-1100-jdbc4",
   "com.microsoft.sqlserver" % "sqljdbc4" % "4.0",
   "org.apache.commons" % "commons-dbcp2" % "2.0.1",
   "org.apache.commons" % "commons-lang3" % "3.1",
   "commons-io" % "commons-io" % "2.4",
   "c3p0" % "c3p0" % "0.9.0.4",
   "org.apache.thrift" % "libthrift" % "0.9.1"
   		exclude("org.slf4j", "slf4j-api") 
   		exclude("org.slf4j", "slf4j-log4j12"),
   "org.apache.kafka" % "kafka_2.11" % "0.8.2.0"
   			exclude("org.slf4j", "slf4j-api") 
   			exclude("org.slf4j", "slf4j-log4j12"),
   	"ly.stealth" % "scala-kafka" % "0.1.0.0"
   		intransitive(),
   	"com.101tec" % "zkclient" % "0.4"
   			exclude("org.slf4j", "slf4j-api") 
   			exclude("org.slf4j", "slf4j-log4j12"),
   	"com.yammer.metrics" % "metrics-core" % "2.2.0"
   			exclude("org.slf4j", "slf4j-api"),
   	"net.sf.jopt-simple" % "jopt-simple" % "4.8",
   	"org.json4s" % "json4s-native_2.11" % "3.2.11",
   	"org.scalikejdbc" % "scalikejdbc_2.11" % "2.2.2"
   	)
   	

testOptions += Tests.Argument(TestFrameworks.JUnit, "-v")

resolvers += Resolver.mavenLocal

net.virtualvoid.sbt.graph.Plugin.graphSettings