package org.cscie88c.lobbyproject

import scala.io.Source
import scala.util.{Try, Success, Failure}

final case class ReportType(
  name: String,
  code: String
)

object ReportType {
  def apply(csvString: String): Option[ReportType] = Try {
    val fields = csvString.replace(",|","##").replace("|,","##").split("##")
    
    ReportType(
      name = fields(0).replace("|", ""),
      code = fields(1).replace("|", "")
    )
  }.toOption

  def readFromCSVFile(fileName: String): List[ReportType] =  
    Source
      .fromResource(fileName)
      .getLines
      .map(ReportType(_))
      .collect {case Some (cus) => cus}
      .toList

}