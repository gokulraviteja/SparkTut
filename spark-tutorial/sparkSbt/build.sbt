lazy val root = (project in file("."))
  .settings(
    name         := "sparkSbt",
    organization := "com.grt",
    scalaVersion := "2.11.8",
    version      := "1.0-SNAPSHOT",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4",
    libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.4",
  )
