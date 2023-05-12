name := "spark-db-connector"

version := "0.1.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.3.2"

val gitCommitString = SettingKey[String]("gitCommit")
gitCommitString := s"${git.gitHeadCommit.value.getOrElse("[No commit Set]")} ${git.gitHeadCommitDate.value
  .getOrElse("[No Date]")} From Branch: ${git.gitCurrentBranch.value})}\n"
lazy val root = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .enablePlugins(GitVersioning)
  .settings(
    //buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoKeys := Seq[BuildInfoKey](version, gitCommitString),
    buildInfoPackage := "attolini.libs.spark.dbConnector",
    buildInfoOptions += BuildInfoOption.ToMap,
    buildInfoOptions += BuildInfoOption.ToJson
  )

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

resolvers := List(
  "Hortonworks Repository" at "https://repo.hortonworks.com/content/repositories/releases/",
  "SparkPackages" at "https://dl.bintray.com/spark-packages/maven/"
)
resolvers += Resolver.sonatypeRepo("snapshots")
resolvers += Resolver.sonatypeRepo("releases")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
//  ("com.hortonworks.hive" %% "hive-warehouse-connector" % "1.0.0.3.1.4.0-315" % "provided")
//    .exclude("org.apache.hadoop", "hadoop-aws"),
  "org.apache.commons" % "commons-lang3"   % "3.5",
  "com.beachape"       %% "enumeratum" % "1.5.15"
)

unmanagedJars in Compile += file("lib")

/* LIBRARIES FOR TEST */
libraryDependencies ++= Seq(
  "org.scalatest"  %% "scalatest"  % "3.1.1"  % Test,
  "org.scalacheck" %% "scalacheck" % "1.14.3" % Test
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x                             => MergeStrategy.first
}

fork in Test := true
test in assembly := {}

assemblyOutputPath in assembly := file(
  "target/scala-2.11/" + name.value + "-" + version.value + ".jar"
)
