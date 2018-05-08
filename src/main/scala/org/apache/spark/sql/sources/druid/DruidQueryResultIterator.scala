package org.apache.spark.sql.sources.druid

import java.io.{ByteArrayInputStream, InputStream}

import org.apache.spark.util.NextIterator
import org.rzlabs.druid.client.{QueryResultRow, ResultRow}
import org.fasterxml.jackson.databind.ObjectMapper._
import com.fasterxml.jackson.core._
import com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.commons.io.IOUtils

private class DruidQueryResultStreamingIterator(useSmile: Boolean,
                                       is: InputStream,
                                       onDone: => Unit = ()
                                      ) extends NextIterator[QueryResultRow]
  with CloseableIterator[QueryResultRow] {

  // In NextIterator the abstract `closeIfNeeded`
  // method declared in CloseableIterator is defined.

  private val (mapper, factory) = if (useSmile) {
    (smileMapper, smileMapper.getFactory)
  } else {
    (jsonMapper, jsonMapper.getFactory)
  }

  private val parser = factory.createParser(is)
  var token = parser.nextToken()  // current token is START_ARRAY
  token = parser.nextToken()  // current token is START_OBJECT or END_ARRAY (empty result set)

  override protected def getNext(): QueryResultRow = {
    if (token == JsonToken.END_ARRAY) {
      finished = true
      null
    } else if (token == JsonToken.START_OBJECT) {
      val r: QueryResultRow = mapper.readValue(parser, new TypeReference[QueryResultRow] {})
      token = parser.nextToken()
      r
    } else null
  }

  override protected def close(): Unit = {
    parser.close()
    onDone
  }
}

private class DruidQueryResultStaticIterator(useSmile: Boolean,
                                             is: InputStream,
                                             onDone: => Unit = ()
                                            ) extends NextIterator[QueryResultRow]
  with CloseableIterator[QueryResultRow] {

  val rowList: List[QueryResultRow] = if (useSmile) {
    val bais = new ByteArrayInputStream(IOUtils.toByteArray(is))
    smileMapper.readValue(bais, new TypeReference[List[QueryResultRow]] {})
  } else {
    jsonMapper.readValue(is, new TypeReference[List[QueryResultRow]] {})
  }

  onDone

  val iter = rowList.toIterator

  override protected def getNext(): QueryResultRow = {
    if (iter.hasNext) {
      iter.next()
    } else {
      finished = true
      null
    }
  }

  override protected def close(): Unit = () // This because the onDone is called in constructor.
}

object DruidQueryResultIterator {

  def apply(useSmile: Boolean,
            is: InputStream,
            onDone: => Unit = (),
            fromList: Boolean = false): CloseableIterator[QueryResultRow] = {
    if(fromList) {
      new DruidQueryResultStaticIterator(useSmile, is, onDone)
    } else {
      new DruidQueryResultStreamingIterator(useSmile, is, onDone)
    }
  }
}

class DummyResultIterator extends NextIterator[ResultRow] with CloseableIterator[ResultRow] {
  override protected def getNext(): ResultRow = {
    finished = true
    null
  }

  override protected def close(): Unit = ()
}
