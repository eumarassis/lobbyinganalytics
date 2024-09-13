package org.cscie88c.lobbyproject

import scala.io.Source
import scala.util.{Try, Success, Failure}

final case class Lobbying(
  uniqid: String,
  registrantRaw: String,
  registrant: String,
  isFirm : Boolean,
  clientRaw : String,
  client : String,
  ultorg : String,
  amount : Double,
  catCode : String,
  source : String,
  self : String,
  includeNSFS : Boolean,
  use : Boolean,
  ind : Boolean,
  year : String,
  typeShort : String,
  typelong : String,
  affiliate : Boolean
)

object Lobbying {
  def apply(csvString: String): Option[Lobbying] = Try {
    //val fields = csvString.split("|,")
    //val fields = csvString.split(""",(?=(?:[^|]|[^|]|)[^|]$)""", -1)
    //val fields = """\|(.*?)\|""".r.findAllIn(csvString).toArray
    val fields = csvString.replace(",|","##").replace("|,","##").split("##")
    
    Lobbying(
      uniqid = fields(0).replace("|", ""),
      registrantRaw = fields(1).replace("|", ""),
      registrant = fields(2).replace("|", ""),
      isFirm = if (fields(3).replace("|", "") == "y") true else false,
      clientRaw = fields(4).replace("|", ""),
      client = fields(5).replace("|", ""),
      ultorg = fields(6).replace("|", ""),
      amount = fields(7).replace("|", "").toDouble,
      catCode = fields(8).replace("|", ""),
      source = fields(9).replace("|", ""),
      self = fields(10).replace("|", ""),
      includeNSFS = if (fields(11).replace("|", "") == "y") true else false,
      use = if (fields(12).replace("|", "") == "y") true else false,
      ind = if (fields(13).replace("|", "") == "y") true else false,
      year = fields(14).replace("|", ""),
      typeShort = fields(15).replace("|", ""),
      typelong = fields(16).replace("|", ""),
      affiliate = if (fields(17).replace("|", "") == "y") true else false
    )
  }.toOption

  def readFromCSVFile(fileName: String): List[Lobbying] =  
    Source
      .fromResource(fileName)
      .getLines
      .map(Lobbying(_))
      .collect {case Some (cus) => cus}
      .toList

}