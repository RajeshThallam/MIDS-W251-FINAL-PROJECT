name := "WikiPageViewStats Project"
version := "1.0"
scalaVersion := "2.10.4"

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
   {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
   }
}

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.4.1"
libraryDependencies += "com.typesafe" % "config" % "1.2.0"
resolvers += "Akka Repository" at "http://repo.akka.io/releases/"