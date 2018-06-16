//package lyd.ai.structured.sck
//
//import java.sql.{Date, Timestamp}
//import java.util.Calendar
//
//import lyd.ai.structured.sck.sink.ClickHouseSinkProvider
//import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
//import org.apache.spark.sql.{Row, SparkSession}
//
//
//object Main {
//  // input events
//  case class Event(word: String, timestamp: Timestamp)
//  // stream internal state
//  case class State(c: Int)
//  // stream output
//  case class StateUpdate(updateTimestamp: Timestamp, word: String, c: Int)
//  // clickhouse sink
//  class ClickHouseStateUpdatesSinkProvider extends ClickHouseSinkProvider[StateUpdate] {
//    override def clickHouseServers: Seq[(String, Int)] = Seq(("localhost", 8123))
//    override def dbName: String = "default"
//    override def tableName = Some("stateUpdates")
//    override def eventDateColumnName: String = "eventDate"
//    override def indexColumns: Seq[String] = Seq("word")
//    override def partitionFunc: (Row) => Date =
//      (row) => {
//        // use event timestamp as partition key
//        new java.sql.Date(row.getAs[Timestamp](0).getTime)
//        // use current
//        //new java.sql.Date(Calendar.getInstance().getTimeInMillis())
//      }
//  }
//
//
//  def main(args: Array[String]): Unit = {
//    // spark session
//    val spark = SparkSession
//      .builder
//      .master("local[*]")
//      .config("spark.sql.streaming.checkpointLocation", "./spark-checkpoints")
//      .appName("streaming-test")
//      .getOrCreate()
//
//    import spark.implicits._
//
//    val host = "localhost"
//    val port = "9999"
//
//    // define socket source
//    val lines = spark.readStream
//      .format("socket")
//      .option("host", host)
//      .option("port", port)
//      .option("includeTimestamp", true)
//      .load()
//
//    // transform input data to stream of events
//    val events = lines
//      .as[(String, Timestamp)]
//      .flatMap { case (line, timestamp) =>
//        line.split(" ").map(word => Event(word = word, timestamp))
//      }
//
//    println(s"Events schema:")
//    events.printSchema()
//
//    // statefull transformation: word => Iterator[Event] => Iterator[StateUpdate]
//    val stateStream = events.groupByKey((x) => x.word)
//      .flatMapGroupsWithState[State, StateUpdate](OutputMode.Append(), GroupStateTimeout.NoTimeout())((key, iter, state) => {
//
//      // get / create new state
//      val wState = state.getOption.getOrElse(State(0))
//      val count = wState.c + iter.length
//
//      // update state
//      state.update(State(count))
//
//      // output: Iterator[StateUpdate]
//      List(
//        StateUpdate(new Timestamp(Calendar.getInstance().getTimeInMillis), key, count)
//      ).toIterator
//    })
//
//    val query = stateStream.writeStream
//      .outputMode("append")
////      .format("ClickHouseEventsSinkProvider") // clickhouse sink
//      .format("console")
//      .start()
//
//    query.awaitTermination()
//  }
//
//
//
//}
