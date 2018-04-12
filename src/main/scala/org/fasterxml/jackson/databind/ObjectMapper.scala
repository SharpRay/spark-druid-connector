package org.fasterxml.jackson.databind

import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.fasterxml.jackson.module.scala._
import com.fasterxml.jackson.datatype.joda._


object ObjectMapper {

  val jsonMapper = {
    val om = new ObjectMapper()
    om.registerModule(DefaultScalaModule)
    om.registerModule(new JodaModule)
    om
  }

  val smileMapper = {
    val om = new ObjectMapper(new SmileFactory())
    om.registerModule(DefaultScalaModule)
    om
  }
}