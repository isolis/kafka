package kafka.tools

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.util.Properties
import scala.util.Random

import kafka.common._
import org.apache.kafka.clients.producer.ProducerRecord

class LineMessageGeneratorReader extends MessageReader {
  var topic: String = null
  var reader: BufferedReader = null
  var keySeparator = "\t"

  override def init(inputStream: InputStream, props: Properties) {
    topic = props.getProperty("topic")
    reader = new BufferedReader(new InputStreamReader(inputStream))
  }

  /* Return a random string
   *
   * This function creates and returns a string made up of random characters of length length
   * @param lenght of the string to be returned
   */
  def randomString(length: Integer) : String = {
    var alphaString = Random.alphanumeric.take(length).mkString
    while(alphaString.length < length) {
      alphaString = alphaString + Random.alphanumeric.take(length - alphaString.length).mkString
    }
    return alphaString
  }

  override def readMessage() = {
    val commandLine = reader.readLine()
    val commandLineArray = commandLine.split(" ",2)
    val command = commandLineArray(0)
    val parameters = if (commandLineArray.length > 1) commandLineArray(1) else null

    (command,parameters) match {
      case ("text", null) => createAndPrintRecord("SampleText" + randomString(2))
      case ("text", _) => createAndPrintRecord(commandLineArray(1))
      case ("random", null) => createAndPrintRecord(randomString(60))
      case ("random",  _) => createAndPrintRecord(randomString(commandLineArray(1).toInt))
      case ("zero",  _) => createAndPrintRecord("test")
      case (_, _) => createAndPrintRecord(commandLine)
    }
  }

  def createAndPrintRecord(messageText: String) : ProducerRecord[Array[Byte],Array[Byte]] = {
    println(messageText.length + " " + messageText + " " + messageText.length)
    new ProducerRecord(topic,messageText.getBytes)
  }
}
