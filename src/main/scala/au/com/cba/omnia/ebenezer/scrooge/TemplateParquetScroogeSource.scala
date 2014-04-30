package au.com.cba.omnia.ebenezer
package scrooge

import au.com.cba.omnia.ebenezer.scrooge._
import com.twitter.scalding._

import cascading.tap.SinkMode
import cascading.tap.Tap
import cascading.tap.hadoop.PartitionTap
import cascading.tap.hadoop.Hfs
import cascading.tap.partition.DelimitedPartition
import cascading.tuple.Tuple
import cascading.tuple.Fields

import com.twitter.scalding._, TDsl._

import cascading.scheme.Scheme
import cascading.tuple.Fields

import com.twitter.scalding._
import com.twitter.scrooge._

import org.apache.thrift._

import parquet.cascading._

case class TemplateParquetScroogeSource[A, T <: ThriftStruct](template: String, path: String)(implicit m : Manifest[T], valueConverter: TupleConverter[T], valueSet: TupleSetter[T], ma : Manifest[A], partitionConverter: TupleConverter[A], partitionSet: TupleSetter[A])
    extends FixedPathSource(path)
    with TypedSink[(A, T)]
    with Mappable[(A, T)]
    with java.io.Serializable {

  /* ☠ DO NOT USE intFields, scalding / cascading Fields.merge is broken and gets called in bowels of TemplateTap. See scalding/#803. */
  def toFields(start: Int, end: Int): Fields =
    Dsl.strFields((start until end).map(_.toString))

  /* ☠ Create the underlying scrooge-parquet scheme and explicitly set the sink fields to be only the thrift struct
       see sinkFields for other half of this work around. ☠ */
  override def hdfsScheme = {
    val scheme = HadoopSchemeInstance(new ParquetScroogeScheme[T].asInstanceOf[Scheme[_, _, _, _, _]])
    scheme.setSinkFields(toFields(0, 1))
    scheme
  }

  /* The template fields, offset by the value arity (which should always be 1, but using the passed in value for consistency / cross-validation) */
  def templateFields =
    toFields(valueSet.arity, valueSet.arity + partitionSet.arity)

  /* ☠ Advertise all the sinkFields, both the value and partition ones, this needs to be like this even
       though it is the incorrect sink fields, otherwise scalding validation falls over, see hdfsScheme
       for other part of tweak to narrow fields back to value again to work around this. ☠ */
  override def sinkFields : Fields =
    toFields(0, valueSet.arity + partitionSet.arity)

  override def createTap(readOrWrite: AccessMode)(implicit mode : Mode): Tap[_,_,_] =
    (mode, readOrWrite) match {
      case (hdfsMode @ Hdfs(_, _), Read) =>
        createHdfsReadTap(hdfsMode)
      case (Hdfs(_, c), Write) =>
        val hfs = new Hfs(hdfsScheme, hdfsWritePath, SinkMode.REPLACE)
        new PartitionTap(hfs, new DelimitedPartition( templateFields ), SinkMode.UPDATE)
      case (_, _) =>
        super.createTap(readOrWrite)(mode)
    }

  /* ☠ Create a converter which is the union of value and partition, it is _not_ safe to pull this out as a generic
       converter, because if anyone forgets to explicitly type annotate the A infers to Any and you get default
       coverters (yes, scala libraries, particularly scalding do this, it is not ok, but we must deal with it),
       so we hide it inside the Source so it can't be messed up. See also setter. ☠ */
  override def converter[U >: (A, T)] =
    TupleConverter.asSuperConverter[(A, T), U](new TupleConverter[(A, T)] {
      import cascading.tuple.TupleEntry

      def arity = valueConverter.arity + partitionConverter.arity

      def apply(te : TupleEntry) : (A, T) = {
        val value = new TupleEntry(toFields(0, valueConverter.arity))
        val partition = new TupleEntry(toFields(0, partitionConverter.arity))
        (0 until valueConverter.arity).foreach(idx => value.setObject(idx, te.getObject(idx)))
        (0 until partitionConverter.arity).foreach(idx => partition.setObject(idx, te.getObject(idx + valueConverter.arity)))
        partitionConverter(partition) -> valueConverter(value)
       }
    })

  /* ☠ Create a setter which is the union of value and partition, it is _not_ safe to pull this out as a generic
       converter, because if anyone forgets to explicitly type annotate the A infers to Any and you get default
       coverters (yes, scala libraries, particularly scalding do this, it is not ok, but we must deal with it),
       so we hide it inside the Source so it can't be messed up. See also converter. ☠ */
  override def setter[U <: (A, T)] =
    TupleSetter.asSubSetter[(A, T), U](new TupleSetter[(A, T)] {

      def arity = valueSet.arity + partitionSet.arity

      def apply(arg: (A, T)) = {
        val (a, t) = arg
        val partition = partitionSet(a)
        val value = valueSet(t)
        val output = Tuple.size(partition.size + value.size)
        (0 until value.size).foreach(idx => output.set(idx, value.getObject(idx)))
        (0 until partition.size).foreach(idx => output.set(idx + value.size, partition.getObject(idx)))
        output
      }
    })
}
