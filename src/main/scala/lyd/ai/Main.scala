package lyd.ai

import java.sql.Timestamp
import java.util.Calendar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}


object Main {


  // input events
  case class Event(word: String, timestamp: Timestamp)

  // stream internal state
  case class State(c: Int)

  // stream output
  case class StateUpdate(updateTimestamp: Timestamp, word: String, c: Int)

  def main(args: Array[String]): Unit = {
      // spark session
      val spark = SparkSession
        .builder
        .master("local[*]")
        .config("spark.sql.streaming.checkpointLocation", "./spark-checkpoints")
        .appName("streaming-test")
        .getOrCreate()

      import spark.implicits._

      val host = "localhost"
      val port = "9999"

      // define socket source
      val lines = spark.readStream
        .format("socket")
        .option("host", host)
        .option("port", port)
        .option("includeTimestamp", true)
        .load()

      // transform input data to stream of events
      val events = lines
        .as[(String, Timestamp)]
        .flatMap { case (line, timestamp) =>
          line.split(" ").map(word => Event(word = word, timestamp))
        }

      println(s"Events schema:")
      events.printSchema()

      // statefull transformation: word => Iterator[Event] => Iterator[StateUpdate]
      val stateStream = events.groupByKey((x) => x.word)
        .flatMapGroupsWithState[State, StateUpdate](OutputMode.Append(), GroupStateTimeout.NoTimeout())((key, iter, state) => {

        // get / create new state
        val wState = state.getOption.getOrElse(State(0))
        val count = wState.c + iter.length

        // update state
        state.update(State(count))

        // output: Iterator[StateUpdate]
        List(
          StateUpdate(new Timestamp(Calendar.getInstance().getTimeInMillis), key, count)
        ).toIterator
      })

      val query = stateStream.writeStream
        .outputMode("append")
        .format("ClickHouseStateUpdatesSinkProvider") // clickhouse sink
        //.format("console")
        .start()

      query.awaitTermination()
    }

  }
