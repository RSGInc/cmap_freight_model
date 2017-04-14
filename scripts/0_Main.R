##############################################################################################
#Title:             CMAP Agent Based Freight Forecasting Code
#Project:           CMAP Agent-based economics extension to the meso-scale freight model
#Description:       0_Main.R controls the model flow and sources in other scripts to run
#                   components of the model. The code as whole implements the national supply
#                   chain freight framework, which is bassed on earlier work for FHWA carried
#                   out by RSG
#Date:              January 28, 2014
#Author:            Resource Systems Group, Inc.
#Copyright:         Copyright 2014 RSG, Inc. - All rights reserved.
##############################################################################################

#---------------------------------------------------------------------
#Define Model/Scenario Control Variables/Inputs/Packages/Steps
#---------------------------------------------------------------------

#1.Set the base directory (the directory in which the model resides)
#basedir <- "E:/cmh/Meso_Freight_PMG_Base_Test_Setup"

getScriptDirectory <- function(systemFrames, debug = TRUE) {
  stopifnot(!is.null(systemFrames))
  debug = TRUE
  scriptPath <- NULL
  possibleFieldNames <- c("fileName", "ofile", "path")
  for (frame in  systemFrames) {
    namesInFrame <- names(frame)
    if (debug) print(paste0("frame has these fields: ", paste0(collapse=", ", namesInFrame)))
    for (possibleFieldName in possibleFieldNames) {
      if (possibleFieldName %in% namesInFrame) {
        scriptPath <- frame[[possibleFieldName]]
        if (debug)
          print(paste0("got scriptPath from '", possibleFieldName, "'"))
        break
      } #end if found filedName
    } #end for loop over possible field names
    if (!is.null(scriptPath)) {
      break
    }
  } #end loop over frames

  if (is.null(scriptPath)) {
    if (debug) print(paste0("Could not find script path in frames so trying to get scriptPath from rstudioapi::getActiveDocumentContext()$path"))
    scriptPath <- rstudioapi::getActiveDocumentContext()$path
  }
  scriptDir <- dirname(scriptPath)
  if (debug)
    print(paste0("scriptPath: '",  scriptPath, "'"))
  if (debug)
    print(paste0("scriptDir: '",  scriptDir, "'"))
  return(scriptDir)
} #end getScriptDirectory

scriptDir <- getScriptDirectory(sys.frames())
basedir <- dirname(scriptDir) #basedir is the root of the github repo -- one above the scripts directory
print(paste0("basedir: ", basedir))

#2. Set the scnario to run -- same as the folder name inside the scenarios directory
scenario <- "base"

#3. Run the model

#rFreight install zip should be in directory within model
if(!dir.exists("./library")){ dir.create("./library") }
install.packages(file.path(basedir,"rFreight_0.1.zip"),repos=NULL, lib = "./library/" )
library(rFreight, lib.loc = "./library/")

#define the components that comprise the model: name, titles, scripts
steps <- c("firmsyn","pmg","pmgcon","pmgout","daysamp","whouse","vehtour","stopseq","stopdur","tourtod","preptt")
steptitles <- c("Firm Synthesis","Procurement Market Games","PMG Controller",
                "PMG Outputs","Daily Sample","Warehouse Allocations",
                "Vehicle Choice and Tour Pattern","Stop Sequence","Stop Duration",
                "Time of Day","Prepare Trip Table")
stepscripts <- c("01_Firm_Synthesis.R","02_Procurement_Markets.R","03_PMG_Controller.R",
                 "04_PMG_Outputs.R","05_Daily_Sample.R","06_Warehouse_Allocation_CMAP.R",
                 "07_Vehicle_Choice_Tour_Pattern_CMAP.R","08_Stop_Sequence_CMAP.R","09_Stop_Duration.R",
                 "10_Time_of_Day.R","11_Prepare_Trip_Table_CMAP.R")

#create the model list to hold that information, load packages, make model step lists
model <- startModel(basedir=basedir,scenarioname=scenario,
                    packages=c("data.table","bit64","reshape","reshape2","ggplot2","fastcluster"),
                    steps=steps, steptitles=steptitles,stepscripts=stepscripts)
rm(basedir,scenario,steps,steptitles,stepscripts)

#Load file paths to model inputs, outputs, and workspaces
source("./scripts/0_File_Locations.R")

#-----------------------------------------------------------------------------------
#Run model steps
#-----------------------------------------------------------------------------------
progressManager("Start",model$logs$Step_RunTimes, model$scenvars$outputlog, model$logs$Main_Log,
                model$scenvars$outputprofile, model$logs$Profile_Log, model$logs$Profile_Summary)

# split this out to run steps seperately more easily
# functionalize the step 3 stuff to allow just certain markets to run
# change 03_PMG_Controller.R to using parallel instead of tasklist approach

#lapply(model$stepscripts,source)

isPMGDevelopmentMode <-
  dir.exists(model$outputdir) && (length(list.files(model$outputdir)) > 10) && interactive() && (Sys.info()[["user"]] == "peter.andrews")
if (isPMGDevelopmentMode) {
  print("NOTICE -- skipping steps 1 & 2 because in isPMGDevelopmentMode")
} else {
  source(model$stepscripts[1]) #Firm Synthesis
  source(model$stepscripts[2]) #Prepare Procurement Markets
}
source(model$stepscripts[3]) #PMG Controller (running the PMGs)
source(model$stepscripts[4]) #PMG Outputs (creating pairs.Rdata)

if (isPMGDevelopmentMode) {
  print("NOTICE -- skipping steps 5:11 because in isPMGDevelopmentMode")
} else {
  source(model$stepscripts[5:11]) #Truck Touring Model
}


save(list=c("model",model$steps),file=file.path(model$outputdir,"modellists.Rdata"))

progressManager("Stop",model$logs$Step_RunTimes, model$scenvars$outputlog, model$logs$Main_Log,
                model$scenvars$outputprofile, model$logs$Profile_Log, model$logs$Profile_Summary)


