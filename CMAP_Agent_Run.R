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

if (is.null(scriptpath) || is.na(scriptpath)) {
  warning(
    "Can not find path of script so must assume that the working directory is set to the project root"
  )
  basedir <- getwd()
} else {
  scriptDir <- dirname(scriptpath)
  basedir <-
    scriptDir #basedir is the root of the github repo -- which this script is in
}
print(paste0("basedir: ", basedir))
#there is code, such as source statments that assume the working directory is set to base
setwd(basedir)

#2. Set the scenario to run -- same as the folder name inside the scenarios directory
scenario <- "base"

#3. Run the model
source(file.path(basedir,"scripts","00_Main.R"))

