##############################################################################################
#Title:             CMAP Agent Based Freight Forecasting Code
#Project:           CMAP Agent-based economics extension to the meso-scale freight model
#Description:       00_Main.R controls the model flow and sources in other scripts to run
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

#' This will return the location of the calling script.
#' Unfortunately this cannot be moved to a utilities script because until we actually have
#' the current script directory we don't know how to source any other file
#' because we want to use relative paths and they have to relative to something KNOWN

getScriptPathFromFrame <- function(frame, debug=FALSE) {
  scriptPath <- NULL
  if (!is.null(frame)) {
    frameAttributes <- attributes(frame)
    if (debug) print(paste0('names(frameAttributes): ', paste0(collapse=", ", names(frameAttributes))))
    possibleFieldNames <- c("srcfile", "filename", "fileName", "ofile")
    namesInFrame <- names(frame)
    if (debug) print(paste0('namesInFrame: ', paste0(collapse=", ", namesInFrame)))
    fieldNamesInFrame <- namesInFrame[namesInFrame %in% possibleFieldNames]
    if (debug) print(paste0('fieldNamesInFrame: ', fieldNamesInFrame))
    for (fieldNameInFrame in fieldNamesInFrame) {
      if (debug) print(paste0(fieldNameInFrame, '=', frame[[fieldNameInFrame]]))
    }
    if (length(fieldNamesInFrame) > 0) {
      fieldName <- fieldNamesInFrame[1]
      scriptPath <- frame[[fieldName]]
      if (debug) print(paste0("Used frame variable '", fieldName, "' to set scriptPath to '",scriptPath, "'."))
    } #end if found fieldName
  } #end if frame not null
  if (debug && is.null(scriptPath)){
    print(paste0("getScriptPathFromFrame returning NULL!"))
  }
  return(scriptPath)
} # getScriptPathFromFrame

getScriptPath <- function(debug = FALSE, sysStatus=sys.status()) {
  scriptPath <- NULL
  sysCallAttributes <-  attributes(sysStatus$sys.calls[[1]])
  print(paste0("names(sysCallAttributes): ", paste0(collapse = ", ", names(
    sysCallAttributes
  ))))
  if ("srcref" %in% names(sysCallAttributes)) {
    srcref <- sysCallAttributes[["srcref"]]
    srcfile <- attr(srcref, "srcfile")
    scriptPath <- srcfile[["filename"]]
    if (debug)
      print(
        paste0(
          "Used srcref in sys.call() attributes to set scriptPath to: '",
          scriptPath,
          "'."
        )
      )
  } else {
    try(silent = TRUE, expr = {
      scriptPath <<- rstudioapi::getActiveDocumentContext()$path
      if (debug)
        print(
          paste0(
            "Used rstudioapi::getActiveDocumentContext()$path to set scriptPath to: '",
            scriptPath,
            "'."
          )
        )
    })
  }
  if (is.null(scriptPath)) {
    #first check in the current frame
    scriptPath <- getScriptPathFromFrame(sys.frame(), debug)
    if (is.null(scriptPath)) {
      #if not found check all frames
      systemFrames <- sysStatus$sys.frames
      if (debug)
        print(paste0("length(systemFrames): ", length(systemFrames)))

      for (frame in  systemFrames) {
        scriptPath <- getScriptPathFromFrame(frame, debug)
        if (!is.null(scriptPath)) {
          break
        }
      } #end loop over frames
    } #end if not in the current frame
  } #end if need to check frames
  if (debug)
    print(paste0(
      "getScriptPath returning scriptPath '",
      scriptPath,
      "'."
    ))
  return(scriptPath)
} #end getScriptPath

getScriptDir <- function(debug = FALSE) {
  #http://stackoverflow.com/a/30306616/283973
  scriptDir <- getSrcDirectory(function(x) {
    x
  })
  print(paste0(
    'script dir from anonymous function: class: ',
    class(scriptDir),
    " value: ",
    scriptDir
  ))
  if (is.character(scriptDir) &&
      (length(scriptDir) > 0) && (nchar(scriptDir) > 0)) {
    if (debug)
      print(paste0(
        "getSrcDirectory of anonymous function found scriptDir '",
        scriptDir,
        "'."
      ))
  } else {
    scriptPath <- getScriptPath(debug)
    if (is.character(scriptDir) &&
        (length(scriptDir) > 0) && (nchar(scriptDir) > 0)) {
      scriptDir <- dirname(scriptPath)
    } else {
      stop("Could not determine script directory!")
    }
  }
  return(scriptDir)
} #end getScriptDir

scriptDir <- getScriptDir(TRUE)
basedir <-
  dirname(scriptDir) #basedir is the root of the github repo -- one above the scripts directory
print(paste0("basedir: ", basedir))
#there is code, such as source statments that assume the working directory is set to base
setwd(basedir)

source("./scripts/00_Utilities.R")

#2. Set the scnario to run -- same as the folder name inside the scenarios directory
scenario <- "base"

#3. Run the model

#rFreight install zip should be in directory within model
if (!dir.exists("./library")) {
  dir.create("./library")
}
install.packages(file.path(basedir, "rFreight_0.1.zip"),
                 repos = NULL,
                 lib = "./library/")
library(rFreight, lib.loc = "./library/")

#define the components that comprise the model: name, titles, scripts
steps <-
  c(
    "firmsyn",
    "pmg",
    "pmgcon",
    "pmgout",
    "daysamp",
    "whouse",
    "vehtour",
    "stopseq",
    "stopdur",
    "tourtod",
    "preptt"
  )
steptitles <-
  c(
    "Firm Synthesis",
    "Procurement Market Games",
    "PMG Controller",
    "PMG Outputs",
    "Daily Sample",
    "Warehouse Allocations",
    "Vehicle Choice and Tour Pattern",
    "Stop Sequence",
    "Stop Duration",
    "Time of Day",
    "Prepare Trip Table"
  )
stepscripts <-
  c(
    "01_Firm_Synthesis.R",
    "02_Procurement_Markets.R",
    "03_PMG_Controller.R",
    "04_PMG_Outputs.R",
    "05_Daily_Sample.R",
    "06_Warehouse_Allocation_CMAP.R",
    "07_Vehicle_Choice_Tour_Pattern_CMAP.R",
    "08_Stop_Sequence_CMAP.R",
    "09_Stop_Duration.R",
    "10_Time_of_Day.R",
    "11_Prepare_Trip_Table_CMAP.R"
  )

#create the model list to hold that information, load packages, make model step lists
model <- startModel(
  basedir = basedir,
  scenarioname = scenario,
  packages = c(
    "data.table",
    "bit64",
    "reshape",
    "reshape2",
    "ggplot2",
    "fastcluster"
  ),
  steps = steps,
  steptitles = steptitles,
  stepscripts = stepscripts
)
rm(basedir, scenario, steps, steptitles, stepscripts)

#Load file paths to model inputs, outputs, and workspaces
source("./scripts/00_File_Locations.R")

isPeterDevelopmentMode <-
  dir.exists(model$outputdir) &&
  (length(list.files(model$outputdir)) > 10) &&
  interactive() &&
  (Sys.info()[["user"]] == "peter.andrews")

#-----------------------------------------------------------------------------------
#Run model steps
#-----------------------------------------------------------------------------------
progressManager(
  "Start",
  model$logs$Step_RunTimes,
  model$scenvars$outputlog,
  model$logs$Main_Log,
  model$scenvars$outputprofile,
  model$logs$Profile_Log,
  model$logs$Profile_Summary
)

# split this out to run steps seperately more easily
# functionalize the step 3 stuff to allow just certain markets to run
# change 03_PMG_Controller.R to using parallel instead of tasklist approach

#lapply(model$stepscripts,source)

if (isPeterDevelopmentMode && exists("ineligible")) {
  print("NOTICE -- skipping steps 1 & 2 because in isPeterDevelopmentMode")
} else {
  source(model$stepscripts[1]) #Firm Synthesis
  source(model$stepscripts[2]) #Prepare Procurement Markets
}
source(model$stepscripts[3]) #PMG Controller (running the PMGs)
source(model$stepscripts[4]) #PMG Outputs (creating pairs.Rdata)

if (isPeterDevelopmentMode) {
  print("NOTICE -- skipping steps 5:11 because in isPeterDevelopmentMode")
} else {
  source(model$stepscripts[5:11]) #Truck Touring Model
}


save(
  list = c("model", model$steps),
  file = file.path(model$outputdir, "modellists.Rdata")
)

progressManager(
  "Stop",
  model$logs$Step_RunTimes,
  model$scenvars$outputlog,
  model$logs$Main_Log,
  model$scenvars$outputprofile,
  model$logs$Profile_Log,
  model$logs$Profile_Summary
)
