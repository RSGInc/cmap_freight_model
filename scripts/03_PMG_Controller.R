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

load(file.path(model$outputdir, "naics_set.Rdata"))
naics_set <-
  subset(naics_set, NAICS %in% model$scenvars$pmgnaicstorun)

if (nrow(naics_set) != length(model$scenvars$pmgnaicstorun)) {
  stop(paste("Some of model$scenvars$pmgnaicstorun were not found in the naics_set. Number requested=",length(model$scenvars$pmgnaicstorun), ", number found=", nrow(naics_set)))
}
for (naics_run_number in 1:nrow(naics_set)) {
  naics <- naics_set$NAICS[naics_run_number]
  groups <- naics_set$groups[naics_run_number]
  sprod <- ifelse(naics_set$Split_Prod[naics_run_number], 1, 0)
  rScriptCmd <-
    paste(
      "Rscript .\\scripts\\03_0a_Supplier_to_Buyer_Costs.R",
      naics,
      groups,
      sprod,
      model$basedir,
      model$outputdir
    )
  print(paste("Starting:", naics))
  exitStatus <- system(rScriptCmd, wait = TRUE)
  if (exitStatus != 0) {
    stop(paste0("ERROR exitStatus: ", exitStatus, " when running '", rScriptCmd, "'"))
  }
} #end for (naics_run_number in 1:nrow(naics_set))

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

for (naics_run_number in 1:nrow(naics_set)) {
  naics <- naics_set$NAICS[naics_run_number]
  groups <- naics_set$groups[naics_run_number]
  sprod <- ifelse(naics_set$Split_Prod[naics_run_number], 1, 0)
  rScriptCmd <-
    paste(
      "Rscript .\\scripts\\03a_Run_PMG.R",
      naics,
      groups,
      sprod,
      model$basedir,
      model$outputdir
    )
  print(paste("Starting:", naics))
  exitStatus <- system(rScriptCmd, wait = TRUE)
  if (exitStatus != 0) {
    stop(paste0("ERROR exitStatus: ", exitStatus, " when running '", rScriptCmd, "'"))
  }
} #end for (naics_run_number in 1:nrow(naics_set))

pmgcon <- progressEnd(pmgcon)
