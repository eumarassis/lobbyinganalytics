package org.cscie88c.lobbyproject

import scala.io.Source
import scala.util.{Try, Success, Failure}

final case class Lobbyist(
  uniqID: String,
  lobbyistRaw: String,
  lobbyist : String,
  lobbyistId : String,
  year : String, 
  cid : String,
  formercongmem : Boolean
)

object Lobbyist {
  def apply(csvString: String): Option[Lobbyist] = Try {
    val fields = csvString.replace(",|","##").replace("|,","##").split("##")
    
    Lobbyist(
      uniqID = fields(0).replace("|", ""),
      lobbyistRaw = fields(1).replace("|", ""),
      lobbyist = fields(2).replace("|", ""),
      lobbyistId = fields(3).replace("|", ""),
      year = fields(4).replace("|", ""),
      cid = fields(5).replace("|", ""),
      formercongmem =  if (fields(6).replace("|", "") == "y") true else false
    )
  }.toOption

  def readFromCSVFile(fileName: String): List[Lobbyist] =  
    Source
      .fromResource(fileName)
      .getLines
      .map(Lobbyist(_))
      .collect {case Some (cus) => cus}
      .toList

}