package org.apache.spark.sql.sources.druid

import java.io.{ByteArrayInputStream, InputStream}

import org.apache.spark.util.NextIterator
import org.rzlabs.druid.client.{QueryResultRow, ResultRow, ScanResultRow}
import org.fasterxml.jackson.databind.ObjectMapper._
import com.fasterxml.jackson.core._
import com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.commons.io.IOUtils

private class DruidScanResultStreamingIterator(useSmile: Boolean,
                                                is: InputStream,
                                                onDone: => Unit = ()
                                               ) extends NextIterator[ScanResultRow]
  with CloseableIterator[ScanResultRow] {

  // In NextIterator the abstract `closeIfNeeded`
  // method declared in CloseableIterator is defined.

  private val (mapper, factory) = if (useSmile) {
    (smileMapper, smileMapper.getFactory)
  } else {
    (jsonMapper, jsonMapper.getFactory)
  }
  private val parser = factory.createParser(is)
  private var token = parser.nextToken()  // START_ARRAY
  token = parser.nextToken()  // START_OBJECT or END_ARRAY (empty result set)

  var lastToken: JsonToken = JsonToken.START_ARRAY

  override protected def getNext(): ScanResultRow = {

    if ((lastToken == JsonToken.START_ARRAY ||
      lastToken == JsonToken.END_OBJECT) && token != JsonToken.END_ARRAY) {
      token = parser.nextToken()  // FIELD_NAME -- segmentId
      token = parser.nextToken()  // VALUE_STRING -- segment name
      token = parser.nextToken()  // FIELD_NAME -- columns
      token = parser.nextToken()  // START_ARRAY -- column array
      val columns: List[String] = mapper.readValue(parser, classOf[List[String]])
      token = parser.nextToken()  // FIELD_NAME -- events
      token = parser.nextToken()  // START_ARRAY -- event array
      token = parser.nextToken()  // START_OBJECT -- event object
    }

    if (token == JsonToken.END_ARRAY && lastToken == JsonToken.END_OBJECT) {
      finished = true
      null
    } else if (token == JsonToken.START_OBJECT) {
      val r: ScanResultRow = ScanResultRow(
        mapper.readValue(parser, classOf[Map[String, Any]]))
      token = parser.nextToken()  // START_OBJECT or END_ARRAY
      if (token == JsonToken.END_ARRAY) {
        lastToken = parser.nextToken() // END_OBJECT
        token = parser.nextToken()  // END_ARRAY or START_OBJECT
      } else {
        lastToken = JsonToken.START_OBJECT
      }
      r
    } else null
  }

  override protected def close(): Unit = {
    parser.close()
    onDone
  }
}

private class DruidScanResultStaticIterator(useSmile: Boolean,
                                             is: InputStream,
                                             onDone: => Unit = ()
                                            ) extends NextIterator[ScanResultRow]
  with CloseableIterator[ScanResultRow] {

  private var rowList: List[ScanResultRow] = List()

  private val (mapper, factory) = if (useSmile) {
    (smileMapper, smileMapper.getFactory)
  } else {
    (jsonMapper, jsonMapper.getFactory)
  }
  private val parser = factory.createParser(is)

  private var token = parser.nextToken()  // START_ARRAY
  token = parser.nextToken()  // START_OBJECT or END_ARRAY
  while (token == JsonToken.START_OBJECT) {
    token = parser.nextToken()  // FIELD_NAME -- segmentId
    token = parser.nextToken()  // VALUE_STRING -- segment name
    token = parser.nextToken()  // FIELD_NAME -- columns
    token = parser.nextToken()  // START_ARRAY -- column array
    val columns: List[String] = mapper.readValue(parser, classOf[List[String]])
    token = parser.nextToken()  // FIELD_NAME -- events
    token = parser.nextToken()  // START_ARRAY -- event array
    val events: List[Map[String, Any]] = mapper.readValue(parser, classOf[List[Map[String, Any]]])
    rowList = rowList ++ events.map(ScanResultRow(_))
    token = parser.nextToken()  // END_OBJECT
    token = parser.nextToken()  // START_OBJECT or END_ARRAY
  }

  // After loop, here will be END_ARRAY token
  assert(parser.nextToken() == JsonToken.END_ARRAY)

  onDone
  parser.close()

  val iter = rowList.toIterator

  override protected def getNext(): ScanResultRow = {
    if (iter.hasNext) {
      iter.next()
    } else {
      finished = true
      null
    }
  }

  override protected def close(): Unit = () // This because the onDone is called in constructor.
}

object DruidScanResultIterator {

  def apply(useSmile: Boolean,
            is: InputStream,
            onDone: => Unit = (),
            fromList: Boolean = false): CloseableIterator[ScanResultRow] = {
    if(fromList) {
      new DruidScanResultStaticIterator(useSmile, is, onDone)
    } else {
      new DruidScanResultStreamingIterator(useSmile, is, onDone)
    }
  }
}

