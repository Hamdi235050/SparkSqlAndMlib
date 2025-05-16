name := "SparkApp"

version := "0.1.0"

scalaVersion := "2.12.15"

// Forcer la compatibilité avec Java 11 (pour Java uniquement)
javacOptions ++= Seq("--release", "11")

// Supprimer -target:jvm-11 (non supporté par Scala 2.12)
scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-encoding", "utf8")

// Dépendances Spark
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.apache.spark" %% "spark-mllib" % "3.5.0"
)

// Pour éviter certaines erreurs avec Spark et les threads de fork
fork in run := true
