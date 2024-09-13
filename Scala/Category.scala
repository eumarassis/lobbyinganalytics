package org.cscie88c.lobbyproject

import scala.io.Source
import scala.util.{Try, Success, Failure}

final case class Category(
  code	 : String,
  name	 : String,
  order : String,
  industry : String,
  sector : String	
)

object Category {
  def apply(csvString: String): Option[Category] = Try {
    val fields = csvString.split("##")
    
    Category(
      code = fields(0).replace("|", ""),
      name = fields(1).replace("|", ""),
      order = fields(2).replace("|", ""),
      industry = fields(3).replace("|", ""),
      sector = fields(4).replace("|", "")
    )
  }.toOption

  def readFromCSVFile(fileName: String): List[Category] =  
    Source
      .fromResource(fileName)
      .getLines
      .map(Category(_))
      .collect {case Some (cus) => cus}
      .toList

}