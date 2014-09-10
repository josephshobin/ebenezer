//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

import sbt._, Keys._

import com.twitter.scrooge.ScroogeSBT._

import au.com.cba.omnia.uniform.core.standard.StandardProjectPlugin._
import au.com.cba.omnia.uniform.core.version.UniqueVersionPlugin._
import au.com.cba.omnia.uniform.dependency.UniformDependencyPlugin._
import au.com.cba.omnia.uniform.thrift.UniformThriftPlugin._
import au.com.cba.omnia.uniform.assembly.UniformAssemblyPlugin._

import au.com.cba.omnia.humbug.HumbugSBT._

object build extends Build {
  val thermometerVersion = "0.5.3-20150211063703-3f20266-CDH5"
  val omnitoolVersion    = "1.6.0-20150211060329-d0909d8-CDH5"

  lazy val standardSettings =
    Defaults.coreDefaultSettings ++
    uniformDependencySettings ++
    uniform.docSettings("https://github.com/CommBank/ebenezer") ++ Seq(
      logLevel in sbtassembly.Plugin.AssemblyKeys.assembly := Level.Error
    )

  lazy val all = Project(
    id = "all",
    base = file("."),
    settings =
      standardSettings
        ++ uniform.ghsettings
        ++ Seq(
          publishArtifact := false
        ),
    aggregate = Seq(core, test, hive)
  )

  lazy val core = Project(
    id = "core",
    base = file("core"),
    settings = 
      standardSettings
        ++ uniform.project("ebenezer", "au.com.cba.omnia.ebenezer")
        ++ uniformThriftSettings
        ++ humbugSettings
        ++ Seq(
          libraryDependencies ++=
            depend.hadoop() ++ depend.scalding() ++ depend.scalaz() ++
            depend.parquet() ++ Seq(
              "au.com.cba.omnia" %% "humbug-core" % "0.4.0-20150212002030-09a79ac-CDH5",
              "au.com.cba.omnia" %% "thermometer" % thermometerVersion % "test"
            ),
          scroogeThriftSourceFolder in Test <<= (sourceDirectory) { _ / "test" / "thrift" / "scrooge" },
          humbugThriftSourceFolder  in Test <<= (sourceDirectory) { _ / "test" / "thrift" / "humbug" },
          parallelExecution in Test := false
        )
  )

  lazy val test = Project(
    id = "test",
    base = file("test"),
    settings =
      standardSettings
        ++ uniform.project("ebenezer-test", "au.com.cba.omnia.ebenezer.test")
        ++ Seq(
          libraryDependencies ++=
            depend.hadoop() ++ depend.omnia("thermometer-hive", thermometerVersion)
        )
  ).dependsOn(hive)

  lazy val hive = Project(
    id = "hive",
    base = file("hive"),
    settings =
      standardSettings
        ++ uniform.project("ebenezer-hive", "au.com.cba.omnia.ebenezer.hive")
        ++ uniformThriftSettings
        ++ Seq(
          libraryDependencies ++=
            depend.hadoop() ++ depend.parquet() ++
            depend.omnia("omnitool-core", omnitoolVersion) ++ 
            depend.omnia("cascading-hive", "1.6.2-20150212002010-ae5bcb0-CDH5") ++
            Seq(
              "au.com.cba.omnia" %% "thermometer-hive"  % thermometerVersion % "test",
              "au.com.cba.omnia" %% "omnitool-core"     % omnitoolVersion    % "test" classifier "tests"
            ),
          parallelExecution in Test := false
        )
  ).dependsOn(core)

  lazy val tools = Project(
    id = "tools",
    base = file("tools"),
    settings =
      standardSettings
        ++ uniform.project("ebenezer-tools", "au.com.cba.omnia.ebenezer.cli")
        ++ uniformThriftSettings
        ++ uniformAssemblySettings
        ++ Seq(
          libraryDependencies ++=
            depend.hadoop() ++ depend.scalaz() ++ depend.testing() ++ depend.parquet() ++ Seq(
              "com.twitter" % "parquet-tools" % depend.versions.parquet exclude("org.apache.hadoop", "hadoop-client")
            )
        )
  ).dependsOn(core % "test->test")

  lazy val example = Project(
    id = "example",
    base = file("example"),
    settings =
      standardSettings
        ++ uniform.project("ebenezer-example", "au.com.cba.omnia.ebenezer.example")
        ++ uniformThriftSettings
        ++ uniformAssemblySettings
        ++ Seq(
          parallelExecution in Test := false,
          libraryDependencies ++=
            depend.hadoop() ++ depend.scalding() ++ depend.parquet() ++
            depend.omnia("thermometer-hive", thermometerVersion)
        )
  ).dependsOn(hive)
   .dependsOn(test % "test")

  lazy val compat = Project(
    id = "compat",
    base = file("compat"),
    settings =
      standardSettings
        ++ uniform.project("ebenezer-compat", "au.com.cba.omnia.ebenezer.compat")
        ++ uniformThriftSettings
        ++ uniformAssemblySettings
        ++ Seq(
          parallelExecution in Test := false,
          libraryDependencies ++=
            depend.hadoop() ++ depend.scalding() ++ depend.parquet() ++
            depend.omnia("thermometer-hive", thermometerVersion)
        )
  ).dependsOn(hive)
   .dependsOn(test % "test")
}
