package cn.ac.iie.dataImpl.dns

import org.apache.spark.util.{AccumulatorV2, LongAccumulator}

import scala.collection.mutable


class ReturnCodeAccumulator extends AccumulatorV2[String,mutable.HashMap[String,Long]]{

  private var returnCodeAccumulator = mutable.HashMap[String,Long]()

  override def isZero: Boolean = {
    returnCodeAccumulator.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    val accumulator = new ReturnCodeAccumulator
    accumulator.returnCodeAccumulator = this.returnCodeAccumulator
    accumulator
  }

  override def reset(): Unit = {
    returnCodeAccumulator.clear()
  }

  override def add(v: String): Unit = {
    if(returnCodeAccumulator.contains(v)){
      returnCodeAccumulator(v) = returnCodeAccumulator(v)+1
    }else{
      returnCodeAccumulator+=(v->1)
    }
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    other match {
      case o: ReturnCodeAccumulator =>
        val it = o.returnCodeAccumulator.iterator
        while (it.hasNext) {
          val (k, v) = it.next()
          if (this.returnCodeAccumulator.contains(k)) {
            val oldvalue = this.returnCodeAccumulator(k)
            this.returnCodeAccumulator+=(k -> (oldvalue + v))
          } else {
            this.returnCodeAccumulator+=(k -> v)
          }
        }
      case _ =>
        throw new UnsupportedOperationException(
          s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }
  }

  override def value: mutable.HashMap[String, Long] = {
    this.returnCodeAccumulator.clone()
  }

  override def copyAndReset(): AccumulatorV2[String, mutable.HashMap[String, Long]] = new ReturnCodeAccumulator
}
