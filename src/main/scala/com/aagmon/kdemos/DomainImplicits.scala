package com.aagmon.kdemos

import io.circe.{Decoder, Encoder, Error}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.parser.decode
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.serialization.Serdes
import io.circe.syntax._


object DomainImplicits
{

  implicit val userEventSerde: Serde[UserEvent] = Serdes.fromFn(
    //serializer
    (userEvent:UserEvent) => {
      implicit val ueEncoder: Encoder[UserEvent] = deriveEncoder[UserEvent]
      userEvent.asJson.noSpaces.getBytes
    },
    // deserializer
    (userEventBytes:Array[Byte]) => {
      implicit val ueDecoder: Decoder[UserEvent] = deriveDecoder[UserEvent]
      val vOrError: Either[Error, UserEvent] = decode(new String(userEventBytes))
      vOrError match {
        case Right(a) => Option(a)
        case Left(error) =>
          println(s"There was an error converting the message $vOrError, $error")
          Option.empty
      }
    }
  )


  implicit val userSessionSerde: Serde[UserSession] = Serdes.fromFn(
    //serializer
    (userEvent:UserSession) => {
      implicit val usEncoder: Encoder[UserSession] = deriveEncoder[UserSession]
      userEvent.asJson.noSpaces.getBytes
    },
    // deserializer
    (userSessionBytes:Array[Byte]) => {
      implicit val usDecoder: Decoder[UserSession] = deriveDecoder[UserSession]
      val vOrError: Either[Error, UserSession] = decode(new String(userSessionBytes))
      vOrError match {
        case Right(a) => Option(a)
        case Left(error) =>
          println(s"There was an error converting the message $vOrError, $error")
          Option.empty
      }
    }
  )

}
