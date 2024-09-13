package org.cscie88c.lobbyproject

import scala.io.Source
import scala.util.{Try, Success, Failure}

final case class Bill(
  bi_d: Int,
  si_id: Int,
  congNo : String,
  billName : String
)

object Bill {
  def apply(csvString: String): Option[Bill] = Try {
    val fields = csvString.replace(",|","##").replace("|,","##").split("##")
    
    Bill(
      bi_d = fields(0).replace("|", "").toInt,
      si_id = fields(1).replace("|", "").toInt,
      congNo = fields(2).replace("|", ""),
      billName = fields(3).replace("|", "")
    )
  }.toOption

  def readFromCSVFile(fileName: String): List[Bill] =  
    Source
      .fromResource(fileName)
      .getLines
      .map(Bill(_))
      .collect {case Some (cus) => cus}
      .toList

}