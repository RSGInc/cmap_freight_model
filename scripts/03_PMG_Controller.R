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

naicscostrun <- 1L #counter
load(file.path(model$outputdir, "naics_set.Rdata"))
naics_set <-
  subset(naics_set, NAICS %in% model$scenvars$pmgnaicstorun)
naicstorun <- nrow(naics_set)

while (naicscostrun <= naicstorun) {
  naics <- naics_set$NAICS[naicscostrun]
  groups <- naics_set$groups[naicscostrun]
  sprod <- ifelse(naics_set$Split_Prod[naicscostrun], 1, 0)
  rScriptCmd <-
    paste(
      "Rscript .\\scripts\\03_0a_Supplier_to_Buyer_Costs.R",
      naics,
      groups,
      sprod,
      model$basedir,
      model$outputdir
    )
  system(rScriptCmd, wait = TRUE)
  print(paste("Starting:", naics))
  naicscostrun <- naicscostrun + 1L
}


### --------------------------------
progressNextStep("Running PMGs")

naicsrun <- 1L #counter
load(file.path(model$outputdir, "naics_set.Rdata"))
naics_set <-
  subset(naics_set, NAICS %in% model$scenvars$pmgnaicstorun)
naicstorun <- nrow(naics_set)

# have all of the input files been prepared? Don't proceed until they have...
costs_files <-
  paste0(rep(naics_set$NAICS, times = naics_set$groups),
         "_g",
         unlist(lapply(naics_set$groups, seq)),
         ".costs.csv")

while (!all(file.exists(file.path(model$outputdir, costs_files)))) {
  print(paste0(
    collapse = ", ",
    file.path(model$outputdir, costs_files),
    " exists?: ",
    file.exists(file.path(model$outputdir, costs_files))
  ))
  print(paste(
    "Waiting for PMG Inputs Files to be Prepared: Current time",
    Sys.time()
  ))
  Sys.sleep(30)
}

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

while (naicsrun <= naicstorun) {
  naics <- naics_set$NAICS[naicsrun]
  groups <- naics_set$groups[naicsrun]
  sprod <- ifelse(naics_set$Split_Prod[naicsrun], 1, 0)
  rScriptCmd <-
    paste(
      "Rscript .\\scripts\\03a_Run_PMG.R",
      naics,
      groups,
      sprod,
      model$basedir,
      model$outputdir
    )
  system(rScriptCmd, wait = TRUE)
  print(paste("Starting:", naics))
  naicsrun <- naicsrun + 1L
} #end while (naicsrun <= naicstorun)



pmgcon <- progressEnd(pmgcon)
