##############################################################################################
#Title:             CMAP Agent Based Freight Forecasting Code
#Project:           CMAP Agent-based economics extension to the meso-scale freight model
#Description:       00_Main.R controls the model flow and sources in other scripts to run
#                   components of the model. The code as whole implements the national supply
#                   chain freight framework, which is based on earlier work for FHWA carried
#                   out by RSG
#Date:              January 28, 2014
#Author:            Resource Systems Group, Inc.
#Copyright:         Copyright 2014 RSG, Inc. - All rights reserved.
##############################################################################################

#---------------------------------------------------------------------
#Define Model/Scenario Control Variables/Inputs/Packages/Steps
#---------------------------------------------------------------------

#1.Set the base directory (the directory in which the model resides)
if (!exists("scenario")) {
  library(envDocument)
  scriptpath <- envDocument::get_scriptpath()

  if (length(scriptpath) < 2) {
    #How to get script directory: http://stackoverflow.com/a/30306616/283973
    scriptDir <- getSrcDirectory(function(x)
      x)

    if (length(scriptDir)  < 2) {
      scriptDir <- getwd()

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
      dirname(scriptdir) #basedir is the root of the github repo -- one above the scripts directory
  }
  print(paste0("basedir: ", basedir))
} #end if baseDir not already set
#there is code, such as source statments that assume the working directory is set to base
setwd(basedir)

#2. Set the scenario to run -- same as the folder name inside the scenarios directory
if (!exists("scenario")) {
  scenario <- "base"
}

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

#Print number of parallel processes that we are set to run
print(paste0("maxcostrscripts: ", model$scenvars$maxcostrscripts))
print(paste0("maxrscriptinstances: ", model$scenvars$maxrscriptinstances))

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

if(!model$scenvars$runSensitivityAnalysis) {
  source(model$stepscripts[1]) #Firm Synthesis
  source(model$stepscripts[2]) #Prepare Procurement Markets
  source(model$stepscripts[3]) #PMG Controller (running the PMGs)
  source(model$stepscripts[4]) #PMG Outputs (creating pairs.Rdata)

  if(runTruckTouringModel){
    for (scriptNumber in 5:11) {
      source(model$stepscripts[scriptNumber]) #Truck Touring Model
    }
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

} else {
  # First change the functions from rFreight package
  loadInputs2 <- function(filelist, inputdir) {
    if (length(filelist) > 0) {
      for (i in 1:length(filelist)) {
        if (!exists(names(filelist)[i]))
          assign(names(filelist)[i], fread(file.path(inputdir, filelist[[i]])), envir = .GlobalEnv)
      }
    }
  }
  environment(loadInputs2) <- environment(loadInputs)
  assignInNamespace("loadInputs",loadInputs2,ns = "rFreight")

  # Running Sensitivity Analysis
  source(model$stepscripts[1]) #Firm Synthesis
  modeCategories <- fread("./DashBoard/mode_description.csv", stringsAsFactors = FALSE)
  setkey(modeCategories, ModeNumber)
  mesoFAFCBPMap <- fread('./DashBoard/meso_faf_map.csv')
  setkey(mesoFAFCBPMap,"MESOZONE")
  source("./sensitivity/sensitivity_variables.R")
  sensitivity_environment <- new.env()
  allPC <- data.table()
  endPMG <- FALSE
  for(choice in exp_design[,`Choice situation`]){
    B0 <- exp_design[choice,B0]
    B1 <- exp_design[choice,B1]
    B2_mult <- exp_design[choice,B2]
    B3_mult <- exp_design[choice,B3]
    B4 <- exp_design[choice,B4]
    B5_mult <- exp_design[choice,B5]
    if(choice==nrow(exp_design)) endPMG <- TRUE
    source(model$stepscripts[2]) #Prepare Procurement Markets
    source(model$stepscripts[3]) #PMG Controller (running the PMGs)
    allPC <- rbind(allPC,get("pc",envir = sensitivity_environment))
  }
  rsgcolordf <- data.frame(red=c(246,0,99,186,117,255,82), green=c(139,111,175,18,190,194,77), blue=c(31,161,94,34,233,14,133), colornames=c("orange","marine","leaf","cherry","sky","sunshine","violet"))

  rsgcolordf <- rsgcolordf %>% mutate(hexValue=rgb(red,green,blue,maxColorValue=255))

  makeMoreColors <- colorRampPalette(rsgcolordf$hexValue)
  modeColors <- data.table(mode=c("Truck","Rail","Water","Air","Multiple","Pipeline","Other","None"),colors=makeMoreColors(8),stringsAsFactors = FALSE,key = "mode")
  modeColors2 <- makeMoreColors(8)
  names(modeColors2) <- c("Truck","Rail","Water","Air","Multiple","Pipeline","Other","None")

  allPC %>% ggplot(aes(x=Distance_Bin))+geom_bar(aes(fill=Mode),position = "fill")+theme_bw()+facet_grid(Commodity_SCTG~B0)+scale_fill_manual("Mode",values = makeMoreColors(6))+scale_x_continuous(breaks = seq(0,75,10),labels = label_distance)+xlab("Distance")

  saveRDS(allPC,file = file.path(model$outputdir,"sensitivity_run1.rds"))
}
