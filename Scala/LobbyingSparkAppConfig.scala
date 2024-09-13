package org.cscie88c.lobbyproject

// write config case class below
case class LobbyingSparkAppConfig (
  name: String, 
  masterUrl: String, 
  lobbyingFile: String, 
  lobbyistFile: String, 
  reportFile: String, 
  industryFile : String, 
  billFile : String, 
  issueFile : String,
  categoryFile : String )

