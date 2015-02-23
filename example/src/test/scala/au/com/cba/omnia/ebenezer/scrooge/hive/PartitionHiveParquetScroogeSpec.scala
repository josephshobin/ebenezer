package au.com.cba.omnia.ebenezer.scrooge.hive

import scala.concurrent.duration._
import scala.util.Random

import com.twitter.bijection.scrooge.CompactScalaCodec
import com.twitter.scalding._

import org.specs2._
import org.specs2.matcher.ThrownExpectations
import org.specs2.time.NoTimeConversions

import parquet.hadoop.ParquetOutputFormat

import au.com.cba.omnia.ebenezer.test._

import au.com.cba.omnia.thermometer.core._, Thermometer._
import au.com.cba.omnia.thermometer.hive._
import au.com.cba.omnia.thermometer.fact.PathFactoid
import au.com.cba.omnia.thermometer.fact.PathFactoids._

// format: OFF
class PartitionHiveParquetScroogeSpec extends ThermometerSpec with NoTimeConversions with HiveSupport with ThrownExpectations { def is = s2"""
Parquet Test
============

Hive support for (thrift scrooge encoded) partitioned parquet tables
  should allow for writing to a sink $write
  and then it should be possible to read from the data that is written to the sink $read

"""
  // format: ON
  val partitionColumns = List("p_domain" -> "string", "p_partition" -> "bigint", "p_batch" -> "string")
  val database = "database"
  val table = "fake_records"

  val rec = ParquetTestRecord(
    "my_table",
    "column1"
  )
  val testRecords = List(rec)
  val changedRecords = List(rec.copy(entityId = "my_new_table"))

  def write = {
    lazy val parquetTestRecordCodec = CompactScalaCodec(ParquetTestRecord)

    val sink = PartitionHiveParquetScroogeSink[(String, Long, String), ParquetTestRecord](
      database,
      table,
      partitionColumns
    )

    ThermometerSource(List(rec)).map {
      rec ⇒ ("rdbms_changes",0L, "test" ) -> rec
    }.write(sink).withFacts(
      List(hiveWarehouse </> "database.db" </> "fake_records" ==> exists) ++ List(
        hiveWarehouse </> "database.db" </> "fake_records"
          </> s"p_domain=rdbms_changes"
          </> "*"
          </> "*"
          </> "*.parquet" ==> records(
            ParquetThermometerRecordReader[ParquetTestRecord],
            testRecords
          )
      ): _*
    )
  }

  def read = withDependency(write) {
    val typedPipe = TypedPipe.from(PartitionHiveParquetScroogeSource[ParquetTestRecord](
      database,
      table,
      partitionColumns
    ))

    val sink = PartitionHiveParquetScroogeSink[(String, Long), SomeOtherRecord](
      database = database,
      table = "some_other_fake_records_table",
      partitionColumns = List("p_entity" -> "string", "p_partition" -> "bigint")
    )

    typedPipe
      .map { rec ⇒
        ("table", 1L) -> SomeOtherRecord("some_string") //rec.copy(entityId = "my_new_table")
      }
      .write(sink)
      .withFacts(
        List(hiveWarehouse </> "database.db" </> "some_other_fake_records_table" ==> exists) ++ List(
        hiveWarehouse </> "database.db" </> "some_other_fake_records_table"
          </> s"p_entity=table"
          </> "p_partition=1"
          </> "*.parquet" ==> records(
            ParquetThermometerRecordReader[SomeOtherRecord],
            List(SomeOtherRecord("some_string"))
          )
      ): _*
      )
  }
}

