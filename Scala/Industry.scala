package org.cscie88c.lobbyproject

import scala.io.Source
import scala.util.{Try, Success, Failure}

final case class Industry(
  client: String,
  sub: String,
  total : Double,
  year : String,
  catCode : String
)

object Industry {
  def apply(csvString: String): Option[Industry] = Try {
    val fields = csvString.replace(",|","##").replace("|,","##").split("##")
    
    Industry(
      client = fields(0).replace("|", ""),
      sub = fields(1).replace("|", ""),
      total = fields(2).replace("|", "").toDouble,
      year = fields(3).replace("|", ""),
      catCode = fields(4).replace("|", "")
    )
  }.toOption

  def readFromCSVFile(fileName: String): List[Industry] =  
    Source
      .fromResource(fileName)
      .getLines
      .map(Industry(_))
      .collect {case Some (cus) => cus}
      .toList

}