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

# #Start logging
# PMGset_Log <-
#   file.path(outputdir, paste0(naics, "_PMGSetup_Log.txt"))
# doSink <- FALSE
#
# if (doSink) {
#   log <- file(PMGset_Log, open = "wt")
#   sink(log, split = T)
#   sink(log, type = "message")
# }

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
##async tools
##########################################
useFuture <- TRUE
debugFuture <- TRUE
source("./scripts/00_Async_Tasks.R")
########################################

getNewLogFilePath <- function(group) {
  newLogFilePath <-
    file.path(outputdir,
              paste0(
                naics,
                "_group_",
                sprintf("%06d", group),
                "_PMGSetup_Log.txt"
              ))
  #create or truncate the file so we can use append below
  file.create(newLogFilePath)
  return(newLogFilePath)
}

#file to hold timings
baseLogFilePath <- getNewLogFilePath(0)

write(
  paste("Starting:", naics, "Current time", Sys.time()),
  file = baseLogFilePath,
  append = TRUE
)
#check whether sampling within the group has been done and if not run that function
if (!"group" %in% names(prodc))
  create_pmg_sample_groups(naics, groups, sprod, baseLogFilePath)



#loop over the groups and prepare the files for running the games
for (g in 1:groups) {
  log_file_path <- getNewLogFilePath(g)
  #if all processors are in use wait until one is available
  while ((numTasksRunning <-
          processRunningTasks(useFuture, debugFuture)) > model$scenvars$maxrscriptinstances) {
    if (debugFuture)
      print(
        paste0(
          Sys.time(),
          ": Waiting for some of the ",
          numTasksRunning,
          "Supplier_to_Buyer_Costs_makeInputs running tasks to finish so can work on remaining ",
          ((groups - g) + 1),
          " tasks. Tasks: ",
          getRunningTasksStatus()
        )
      )
    Sys.sleep(30)
  } #end while waiting for free slots
  taskName <-       paste0(
    "Supplier_to_Buyer_Costs_makeInputs_naics-",
    naics,
    "_group-",
    g,
    "_of_",
    groups,
    "_sprod-",
    sprod
  )
  write(paste0(Sys.time(), ": Starting ", taskName),
        file = baseLogFilePath,
        append = TRUE)
  startAsyncTask(
    taskName,
    future({
      # model$Current_Commodity <-
      #   naics		## Heither, 12-01-2015: store current NAICS value

      ## Heither, revised 10-05-2015: File Cleanup if Outputs folder being re-used from previous run
      ## -- Delete NAICS_gX.sell file if exists from prior run (existence will prevent create_pmg_inputs from running)
      if (file.exists(file.path(outputdir, paste0(naics, "_g", g, ".sell.csv")))) {
        file.remove(file.path(outputdir, paste0(naics, "_g", g, ".sell.csv")))
      }

      if (!file.exists(file.path(outputdir, paste0(naics, "_g", g, ".sell.csv")))) {
        msg <- paste("Starting:",
                     naics,
                     "Group",
                     g,
                     "Current time",
                     Sys.time())
        write(msg, file = log_file_path, append = TRUE)

        msg <- print(paste(
          "Making Inputs:",
          naics,
          "Group",
          g,
          "Current time",
          Sys.time()
        ))
        write(msg, file = log_file_path, append = TRUE)
        recycle_check_file_path <-
          file.path(
            model$outputdir,
            paste0(
              "recycle_check_naics-",
              naics,
              "_group-",
              g,
              "_initial.txt"
            )
          )
        file.create(recycle_check_file_path)
        create_pmg_inputs(naics, g, sprod, recycle_check_file_path, log_file_path)

        if (!model$scenvars$pmglogging) {
          pmggrouptimes <-
            file(file.path(outputdir, paste0(naics, "_g", g, ".txt")), open = "wt")
          writeLines(print(msg),
                     con = pmggrouptimes)
        }
      }
      return(NULL) #no need to return anything to future task handler
    }),
    callback = function(asyncResults) {
      # asyncResults is: list(asyncTaskName,
      #                        taskResult,
      #                        startTime,
      #                        endTime,
      #                        elapsedTime,
      #                        caughtError,
      #                        caughtWarning)
      write(
        paste0(
          Sys.time(),
          ": Finished ",
          asyncResults[["asyncTaskName"]],
          "Running time:",
          asyncResults[["elapsedTime"]]
        ),
        file = baseLogFilePath,
        append = TRUE
      )
    },
    debug = debugFuture
  ) #end call to startAsyncTask
} #end loop over groups

#wait until all tasks are finished
while ((numrscript <-
        processRunningTasks(useFuture, debugFuture)) > 0) {
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
write(print(
  paste(
    "Completed Supplier_to_Buyer_Costs Making Inputs:",
    naics,
    "Current time",
    Sys.time()
  )
), file = baseLogFilePath, append = TRUE)

quit(save = "no", status = 0) #set status
