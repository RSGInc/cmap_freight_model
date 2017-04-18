## 03_0a_Supplier_to_Buyer_Costs.Rdata
## Heither, CMAP - 07-27-2015

#-----------------------------------------------------------------------------------
#Step 3_0a Create Supplier to Buyer Costs
#-----------------------------------------------------------------------------------
#which NAICS should be run, in how many groups, and do we split producers?
#what are the paths to the base directory of the model and to the current scenario?
args <- commandArgs(TRUE)
naics <- args[1]
groups <- as.integer(args[2])
sprod <- as.integer(args[3])
basedir <- args[4]
outputdir <- args[5]

setwd(basedir)

#Start logging
PMGset_Log <- file.path(outputdir, paste0(naics, "_PMGSetup_Log.txt"))
log <- file(PMGset_Log, open = "wt")
sink(log, split = T)
sink(log, type = "message")

#load the pmg workspace
load(file.path(outputdir, "PMG_Workspace.Rdata"))

#load the workspace for this naics code
load(file.path(outputdir, paste0(naics, ".Rdata")))

#load required packages
library(rFreight, lib.loc = "./library/")
loadPackage("data.table")
loadPackage("reshape")
loadPackage("reshape2")
options(datatable.auto.index = FALSE)

#check whether sampling within the group has been done and if not run that function
if (!"group" %in% names(prodc))
  create_pmg_sample_groups(naics, groups, sprod)

#file to hold timings
setuptimes <-
  file(file.path(outputdir, paste0(naics, "_PMGSetup_Log.txt")), open = "wt")
writeLines(paste("Starting:", naics, "Current time", Sys.time()), con =
             setuptimes)

##async tools
##########################################
useFuture <- TRUE
debugFuture <- TRUE
source("./scripts/00_Async_Tasks.R")
########################################

#loop over the groups and prepare the files for running the games
for (g in 1:groups) {
  numTasksRunning <- processRunningTasks(useFuture, debugFuture)

  #is this less than max to run at once?
  #If yes, run one more, if the next is available, else wait and then check
  if (numTasksRunning < model$scenvars$maxcostrscripts) {
    startAsyncTask(
      paste0(
        "Supplier_to_Buyer_Costs_makeInputs_naics-",
        naics,
        "_group-",
        g,
        "_of_",
        groups,
        "_sprod-",
        sprod
      ),
      future({
        # model$Current_Commodity <-
        #   naics		## Heither, 12-01-2015: store current NAICS value
        model$recycle_check <-
          file.path(model$outputdir, "recycle_check_initial.txt")

        ## Heither, revised 10-05-2015: File Cleanup if Outputs folder being re-used from previous run
        ## -- Delete NAICS_gX.sell file if exists from prior run (existence will prevent create_pmg_inputs from running)
        if (file.exists(file.path(outputdir, paste0(naics, "_g", g, ".sell.csv")))) {
          file.remove(file.path(outputdir, paste0(naics, "_g", g, ".sell.csv")))
        }

        if (!file.exists(file.path(outputdir, paste0(naics, "_g", g, ".sell.csv")))) {
          msg <- paste(
            "Starting:",
            naics,
            "Group",
            g,
            "Current time",
            Sys.time()
          )
          writeLines(print(msg),
          con = setuptimes)

          writeLines(print(paste(
            "Making Inputs:",
            naics,
            "Group",
            g,
            "Current time",
            Sys.time()
          )),
          con = setuptimes)
          create_pmg_inputs(naics, g, sprod)

          if (!model$scenvars$pmglogging) {
            pmggrouptimes <-
              file(file.path(outputdir, paste0(naics, "_g", g, ".txt")), open = "wt")
            writeLines(print(msg),
            con = pmggrouptimes)
          }
        }
      }),
      debug = debugFuture
    ) #end call to startAsyncTask
  } #end if room to add another running task
  else {
    if (debugFuture)
      print(
        paste0(
          Sys.time(),
          ": Waiting for some of the ",
          numrscript,
          " Supplier_to_Buyer_Costs_makeInputs running tasks to finish so can work on remaining ",
          ((groups - g) + 1),
          " tasks. Tasks: ",
          getRunningTasksStatus()
        )
      )
    Sys.sleep(30)
  }
} #end loop over groups

#wait until all tasks are finished
while ((numrscript <- processRunningTasks()) > 0) {
  if (debugFuture)
    print(
      paste0(
        Sys.time(),
        ": Waiting for final ",
        numrscript,
        " Supplier_to_Buyer_Costs_makeInputs tasks to finish."
      )
    )
  Sys.sleep(30)
} #end while final Supplier_to_Buyer_Costs_makeInputs tasks are still running

#close off logging
writeLines(print(paste(
  "Completed Processing Outputs:",
  naics,
  "Current time",
  Sys.time()
)),
con = setuptimes)
close(setuptimes)

#end sinking
sink(type = "message")
sink()

quit(save="no", status=0) #set status
