##############################################################################################
#Title:             CMAP Agent Based Freight Forecasting Code
#Project:           CMAP Agent-based economics extension to the meso-scale freight model
#Description:       CMAP_Agent_Run.R calls the model code for the selected scenario
#Date:              December 12, 2014
#Author:            Resource Systems Group, Inc.
#Copyright:         Copyright 2014 RSG, Inc. - All rights reserved.
##############################################################################################

#1.Set the base directory (the directory in which the model resides)
library(envDocument)
scriptpath <- envDocument::get_scriptpath()
#print(paste("envDocument::get_scriptpath():", envDocument::get_scriptpath()))

if (length(scriptpath) < 2) {
  #How to get script directory: http://stackoverflow.com/a/30306616/283973
  scriptDir <- getSrcDirectory(function(x)
    x)

  if (length(scriptDir)  < 2) {
    scriptDir <- getwd()
    #print(paste("getwd():", getwd()))
    if (!file.exists("cmap_freight_model.Rproj"))
      stop(
        paste0(
          "Can not find path of script and the working directory '",
          scriptDir,
          "' does not appear to be the project root!"
        )
      )
  }
  basedir <- scriptDir
} else {
  scriptdir <- dirname(scriptpath)
  basedir <-
    scriptdir #basedir is the root of the github repo -- one above the scripts directory
}
print(paste0("basedir: ", basedir))
#there is code, such as source statments that assume the working directory is set to base
setwd(basedir)

#2. Set the scenario to run -- same as the folder name inside the scenarios directory
scenario <- "base"

#3. Run the model
source(file.path(basedir, "scripts", "00_Main.R"))
