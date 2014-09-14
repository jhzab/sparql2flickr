name := "sparql2flickr"

version := "0.1"

scalaVersion := "2.11.2"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.2"

libraryDependencies += "org.scribe" % "scribe" % "1.3.5"

libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.3.2"

libraryDependencies += "org.apache.jena" % "jena-arq" % "2.12.0"

libraryDependencies += "org.mongodb" % "casbah-core_2.11" % "2.7.3"

unmanagedBase := baseDirectory.value / "lib"

libraryDependencies <++= (scalaVersion)(sv =>
  Seq(
    "org.scala-lang" % "scala-reflect" % "2.11.2",
    "org.scala-lang" % "scala-compiler" % "2.11.2"
  )
)
