##############################################################################################
#Title:             CMAP Agent Based Freight Forecasting Code
#Project:           CMAP Agent-based economics extension to the meso-scale freight model
#Description:       3_PMG_Controller.R produces the inputs to the PMGs and controls running
#                   the PMGs and cleaning up after them
#Date:              June 30, 2014
#Author:            Resource Systems Group, Inc.
#Copyright:         Copyright 2014 RSG, Inc. - All rights reserved.
##############################################################################################


#-----------------------------------------------------------------------------------
#Step 3 PMG Controller
#-----------------------------------------------------------------------------------
progressStart(pmgcon, 3)

#------------------------------------------------------------------------------------------------------
#Produce PMG inputs and run PMGs
#------------------------------------------------------------------------------------------------------
progressNextStep("Producing Supplier to Buyer Costs")

rScriptCmd <-
  paste(
    "Rscript .\\scripts\\03_0a_Supplier_to_Buyer_Costs.R",
    model$basedir,
    model$outputdir
  )

startTime <- Sys.time()
print(paste0(startTime, ": Starting rScriptCmd: ", rScriptCmd))
exitStatus <- system(rScriptCmd, wait = TRUE)
finishTime <- Sys.time()
print(paste0(
  finishTime,
  ": Finished rScriptCmd: ",
  rScriptCmd,
  ". Time to run: ",
  format(finishTime - startTime)
))
if (exitStatus != 0) {
  stop(paste0(
    "ERROR exitStatus: ",
    exitStatus,
    " when running '",
    rScriptCmd,
    "'"
  ))
}

if(model$scenvars$distchannelCalibration){
  filelistmfg <- list.files(model$inputdir,pattern = "_g\\d+_model_distchannel_mfg",full.names = TRUE,recursive = FALSE)
  if(length(filelistmfg)>0){
    mfg_cal <- rbindlist(lapply(filelistmfg, fread, stringsAsFactors=FALSE))
    saveRDS(mfg_cal,file=file.path(model$inputdir,"model_distchannel_mfg_cal.rds"))
    mfg_calreduced <- mfg_cal[,.(COEFF=weighted.mean(COEFF,weight)),by=.(CATEGORY,CHID,CHDESC,VAR,TYPE)]
    fwrite(mfg_calreduced,file=file.path(model$inputdir,"model_distchannel_mfg_cal.csv"))
    file.remove(filelistmfg)
  }
  filelistfood <- list.files(model$inputdir,pattern = "_g\\d_model_distchannel_food")
  if(length(filelistfood)>0){
    food_cal <- rbindlist(lapply(filelistfood, fread, stringsAsFactors=FALSE))
    saveRDS(food_cal,file=file.path(model$inputdir,"model_distchannel_mfg_cal.rds"))
    food_calreduced <- food_cal[,.(COEFF=weighted.mean(COEFF,weight)),by=.(CATEGORY,CHID,CHDESC,VAR,TYPE)]
    fwrite(food_calreduced,file=file.path(model$inputdir,"model_distchannel_food_cal.csv"))
  }
} else if (model$scenvars$modechoiceCalibration){
  filelist <- list.files(model$inputdir,pattern = "_g\\d+_modechoiceconstants.rds",full.names = TRUE,recursive = FALSE)
  modeChoiceConstants <- rbindlist(lapply(filelist,readRDS))
  saveRDS(modeChoiceConstants,file = file.path(model$inputdir,"allModeChoiceConstants.rds"))
  modeChoiceConstants <- modeChoiceConstants[,.(Constant=mean(Constant)),by=.(Commodity_SCTG,ODSegment,Mode.Domestic)]
  saveRDS(modeChoiceConstants,file = file.path(model$inputdir,"ModeChoiceConstants.rds"))
}

if(model$scenvars$runSensitivityAnalysis & model$scenvars$runParameters){
  # Create directory to collect the output
  if(!dir.exists(file.path(model$parameterdir,paste0("100_",model$parameterdesign)))){
    dir.create(file.path(model$parameterdir,paste0("100_",model$parameterdesign)))
  }

  # Collect all the output before it is deleted
  if(!exists("sensitivity_environment")) sensitivity_environment <- new.env(parent = .GlobalEnv)
  sensitivity_environment$pcbefore <- list()

  # Run it for all NAICS group
  for (naics in model$scenvars$pmgnaicstorun) {
    # sensitivity_environment$pcbefore[[match(naics,model$scenvars$pmgnaicstorun)]] <- rbindlist(lapply(1:naics_set$groups[which(naics_set$NAICS==naics)],function(group) get(load(file.path(
    #   model$outputdir, paste0(naics, "_g", group, ".Rdata")))
    # )))
    pcBefore <- rbindlist(lapply(1:naics_set$groups[which(naics_set$NAICS==naics)],function(group) get(load(file.path(
      model$outputdir, paste0(naics, "_g", group, ".Rdata")))
    )))
    save(pcBefore,file = file.path(
      model$parameterdir,paste0("100_",model$parameterdesign), paste0(naics,"prePMG.Rdata")))
    rm(pcBefore)
  }

  # sensitivity_environment$pcbefore <- rbindlist(sensitivity_environment$pcbefore)
  # sensitivity_environment$pcbefore[, Mode := modeCategories[.(MinPath), Mode]]
  # sensitivity_environment$pcbefore[, ':='(B0 = B0, B1 = B1, B2 = B2_mult, B3 = B3_mult, B4 = B4, B5 = B5_mult, Run = "Pre PMG")]
} else if (model$scenvars$runSensitivityAnalysis){
  if(!exists("sensitivity_environment")) sensitivity_environment <- new.env(parent = .GlobalEnv)
  sensitivity_environment$pcbefore <- list()
  for (naics in model$scenvars$pmgnaicstorun) {
    sensitivity_environment$pcbefore[[match(naics,model$scenvars$pmgnaicstorun)]] <- rbindlist(lapply(1:naics_set$groups[which(naics_set$NAICS==naics)],function(group) get(load(file.path(
      model$outputdir, paste0(naics, "_g", group, ".Rdata")))
    )))
  }
  if(!dir.exists(file.path(model$outputdir,"skimsoutput",basename(model$skimsdir)))){
    dir.create(file.path(model$outputdir,"skimsoutput",basename(model$skimsdir)))
  }
  sensitivity_environment$pcbefore <- rbindlist(sensitivity_environment$pcbefore)
  sensitivity_environment$pcbefore[, Mode := modeCategories[.(MinPath), Mode]]
  sensitivity_environment$pcbefore[, ':='(SkimFile = as.integer(unlist(strsplit(basename(model$skimsdir),"_"))[2]), Run = "Pre PMG")]
}


### --------------------------------
progressNextStep("Running PMGs")

load(file.path(model$outputdir, "naics_set.Rdata"))
naics_set <-
  subset(naics_set, NAICS %in% model$scenvars$pmgnaicstorun)

#Call the writePMGini function to write out the variables above to the PMG ini file at run time
writePMGini(model$scenvars, "./PMG/PMG.ini")

#start monitoring
if (model$scenvars$pmgmonitoring)
  system(
    paste(
      "Rscript .\\scripts\\03b_Monitor_PMG.R",
      model$basedir,
      model$outputdir
    ),
    wait = FALSE
  )

rScriptCmd <-
  paste("Rscript .\\scripts\\03a_Run_PMG.R",
        model$basedir,
        model$outputdir)

startTime <- Sys.time()
print(paste0(startTime, ": Starting rScriptCmd: ", rScriptCmd))
exitStatus <- system(rScriptCmd, wait = TRUE)
finishTime <- Sys.time()
print(paste0(
  finishTime,
  ": Finished rScriptCmd: ",
  rScriptCmd,
  ". Time to run: ",
  format(finishTime - startTime)
))
if (exitStatus != 0) {
  stop(paste0(
    "ERROR exitStatus: ",
    exitStatus,
    " when running '",
    rScriptCmd,
    "'"
  ))
}

if(model$scenvars$runSensitivityAnalysis & model$scenvars$runParameters){
  # Collect all the output after running the PMGs
  if(!exists("sensitivity_environment")) sensitivity_environment <- new.env(parent = .GlobalEnv)
  returnPairsTableNAICS <- function(naics){
    temp <- new.env()
    load(
      file.path(model$outputdir, paste0(naics, ".Rdata")), temp)
    pcAfter <- temp$pairs
    save(pcAfter,file = file.path(model$parameterdir,paste0("100_",model$parameterdesign), paste0(naics,"postPMG.Rdata")))
    # return(temp$pairs)
  }
  lapply(model$scenvars$pmgnaicstorun, returnPairsTableNAICS)
  # sensitivity_environment$pcafter <- rbindlist(lapply(model$scenvars$pmgnaicstorun, returnPairsTableNAICS))
  # sensitivity_environment$pcafter[, Mode := modeCategories[.(MinPath), Mode]]
  # sensitivity_environment$pcafter[, ':='(B0 = B0, B1 = B1, B2 = B2_mult, B3 = B3_mult, B4 = B4, B5 = B5_mult, Run = "Post PMG")]
  # selectColumns <- intersect(colnames(sensitivity_environment$pcbefore),colnames(sensitivity_environment$pcafter))
  # sensitivity_environment$pc <- rbind(sensitivity_environment$pcbefore[,selectColumns,with=FALSE],sensitivity_environment$pcafter[,selectColumns,with=FALSE])
  # rm(pcafter,pcbefore,envir = sensitivity_environment)
} else if(model$scenvars$runSensitivityAnalysis){
  # Collect all the output after running the PMGs
  if(!exists("sensitivity_environment")) sensitivity_environment <- new.env(parent = .GlobalEnv)
  returnPairsTableNAICS <- function(naics){
    temp <- new.env()
    load(
      file.path(model$outputdir, paste0(naics, ".Rdata")), temp)
    return(temp$pairs)
  }
  sensitivity_environment$pcafter <- rbindlist(lapply(model$scenvars$pmgnaicstorun, returnPairsTableNAICS))
  sensitivity_environment$pcafter[, Mode := modeCategories[.(MinPath), Mode]]
  sensitivity_environment$pcafter[, ':='(SkimFile = as.integer(unlist(strsplit(basename(model$skimsdir),"_"))[2]), Run = "Post PMG")]
  selectColumns <- intersect(colnames(sensitivity_environment$pcbefore),colnames(sensitivity_environment$pcafter))
  sensitivity_environment$pc <- rbind(sensitivity_environment$pcbefore[,selectColumns,with=FALSE],sensitivity_environment$pcafter[,selectColumns,with=FALSE])
  if(!dir.exists(file.path(model$outputdir,"skimsoutput",basename(model$skimsdir)))){
    dir.create(file.path(model$outputdir,"skimsoutput",basename(model$skimsdir)))
  }
  save(pc,file = file.path(model$outputdir,"skimsoutput",basename(model$skimsdir),"pc.RData"),envir = sensitivity_environment)
  rm(pcafter,pcbefore,pc,envir = sensitivity_environment)
}

if(!exists("endPMG")) endPMG <- TRUE
if(endPMG) pmgcon <- progressEnd(pmgcon)
