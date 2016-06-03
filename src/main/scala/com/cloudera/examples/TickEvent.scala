package com.cloudera.examples


//"ticket per date time last vol"
class TickEvent(var tickId:String = "",
                var period:String = "",
                var day:String = "",
                var time:String = "",
                var last:String = "",
                var volume:String = "",
                var ts:Long = 0,
                var ts_received:Long = 0,
                var hasChanged:Boolean = false) extends Serializable {

  override def toString():String = {
    tickId + "," +
    period + "," +
    day + "," +
    time + "," +
    last + "," +
    volume + "," +
    ts + "," +
    ts_received + "," +
    hasChanged
  }

  def += (tick: TickEvent): Unit = {
    tickId = tick.tickId
    period = tick.period
    day = tick.day
    time = tick.time
    last = tick.last
    volume = tick.volume
    ts = tick.ts
    ts_received = tick.ts_received
    hasChanged = tick.hasChanged
  }
}

object TickEventBuilder extends Serializable  {
  def build(input:String):TickEvent = {
    val parts = input.split(",")


    new TickEvent(parts(0),
      parts(1),
      parts(2),
      parts(3),
      parts(4),
      parts(5))
  }
}