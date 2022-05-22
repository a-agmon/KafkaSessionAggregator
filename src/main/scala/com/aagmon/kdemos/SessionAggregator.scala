package com.aagmon.kdemos

import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.scala.serialization.Serdes
// Brings all implicit conversions in scope
import org.apache.kafka.streams.scala.ImplicitConversions._
// Bring implicit default serdes in scope
import org.apache.kafka.streams.scala.serialization.Serdes._
import DomainImplicits._

import java.util.Properties

object SessionAggregator {

  def aggregateSession(sessionId:String, userEvent:UserEvent, userSession:UserSession)  = {
    val session = userEvent match {
      case e if e.eventType == "SESSION_START" =>
        userSession.copy(sessionId = sessionId, eventStart = userEvent.eventTime)
      case e if e.eventType == "SESSION_DESTROY" =>
        userSession.copy(sessionId = sessionId, eventEnd = userEvent.eventTime)
      case _ => userSession
    }

    if (session.eventStart > 0 && session.eventEnd > 0){
      session.copy(state = "COMPLETED")
    }else {
      session
    }

  }

  def getProperties(appID:String):Properties = {
    val bootstrapServer:String = scala.util.Properties.envOrElse("KAFKA_SERVER", "127.0.0.1:9092")
    val conf = new Properties
    conf.put(StreamsConfig.APPLICATION_ID_CONFIG, appID)
    conf.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
    conf.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)
    conf.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)
    conf
  }

  def getTopology(sessionsTopic:String, outputTopic:String): Topology ={
    val builder = new StreamsBuilder()
    val sessionStream: KStream[String, UserEvent] =
      builder.stream[String, UserEvent](sessionsTopic)

    val newUserSession = new UserSession(sessionId = "", eventStart = 0, eventEnd = 0, state = "START")
    sessionStream
      .groupByKey
      .aggregate(newUserSession)(aggregateSession)
      .toStream
      .filter((sessionID, session) => {
        session.state == "COMPLETED"
      })
      .to(outputTopic)

    builder.build()
  }
  def main(args: Array[String]): Unit = {

    val topology: Topology =
      getTopology("sessions-input-2", "sessions-output-1")
    val streams: KafkaStreams =
      new KafkaStreams(topology, getProperties("sessions-aggr1-1"))

    streams.cleanUp() // only for test
    streams.start()

    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = {
        streams.close()
      }
    })
  }
}
