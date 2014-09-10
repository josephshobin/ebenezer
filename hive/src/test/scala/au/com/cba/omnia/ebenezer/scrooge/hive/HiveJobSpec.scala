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

package au.com.cba.omnia.ebenezer.scrooge
package hive

import com.twitter.scalding._

import au.com.cba.omnia.thermometer.core._, Thermometer._
import au.com.cba.omnia.thermometer.hive.HiveSupport

object HiveJobSpec extends ThermometerSpec with HiveSupport { def is = sequential ^ s2"""
  HiveJob
  =======

  Can run hive query without any input or output taps $empty

"""

  def empty = {
    val job = withArgs(Map("db" -> "test", "table" -> "dst"))(new EmptyHiveJob(_))

    job.runsOk
  }
}

class EmptyHiveJob(args: Args) extends CascadeJob(args) {
  val db    = args("db")
  val table = args("table")

  val jobs = List(HiveJob(
    args, "empty hive job",
    List.empty, None,
    s"CREATE DATABASE $db",
    s"CREATE TABLE $db.$table (id int)",
    s"SELECT COUNT(*) FROM $db.$table"
  ))
}
