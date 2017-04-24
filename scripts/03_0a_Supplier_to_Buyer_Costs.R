## 03_0a_Supplier_to_Buyer_Costs.Rdata
## Heither, CMAP - 07-27-2015

#-----------------------------------------------------------------------------------
#Step 3_0a Create Supplier to Buyer Costs
#-----------------------------------------------------------------------------------
#which NAICS should be run, in how many groups, and do we split producers?
#what are the paths to the base directory of the model and to the current scenario?
args <- commandArgs(TRUE)
basedir <- args[1]
outputdir <- args[2]

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
debugFuture <- TRUE
source("./scripts/FutureTaskProcessor.R")
#only allocate as many workers as we need (including one for future itself) or to the specified maximum
plan(multiprocess,
     workers = min((groups+1), model$scenvars$maxcostrscripts))
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
  create_pmg_sample_groups(naics, groups, sprod)


runningTasks <- list()

load(file.path(model$outputdir, "naics_set.Rdata"))
naics_set <-
  subset(naics_set, NAICS %in% model$scenvars$pmgnaicstorun)

for (naics_run_number in 1:nrow(naics_set)) {
  naics <- naics_set$NAICS[naics_run_number]
  groups <- naics_set$groups[naics_run_number]
  sprod <- ifelse(naics_set$Split_Prod[naics_run_number], 1, 0)
  print(paste0(Sys.time(),": Starting naics: ", naics))
  #loop over the groups and prepare the files for running the games
  for (g in 1:groups) {
    log_file_path <- getNewLogFilePath(g)

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
        ## Heither, revised 10-05-2015: File Cleanup if Outputs folder being re-used from previous run
        ## -- Delete NAICS_gX.sell file if exists from prior run (existence will prevent create_pmg_inputs from running)
        if (file.exists(file.path(outputdir, paste0(naics, "_g", g, ".sell.csv")))) {
          file.remove(file.path(outputdir, paste0(naics, "_g", g, ".sell.csv")))
        }

        if (!file.exists(file.path(outputdir, paste0(naics, "_g", g, ".sell.csv")))) {
          msg <- print(paste("Starting:",
                             naics,
                             "Group",
                             g,
                             "Current time",
                             Sys.time()))
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
          create_pmg_inputs(naics, g, sprod, recycle_check_file_path)

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
        #check that cost files was create
        taskName <- asyncResults[["asyncTaskName"]]
        naics_and_group_string <-
          gsub("^.*naics[-]([^_]+)_group[-]([^_]+)_.*$",
               "\\1 \\2",
               taskName)
        naics_and_group <-
          strsplit(naics_and_group_string, split = " ")[[1]]
        taskNaics <- naics_and_group[[1]]
        taskGroup <- naics_and_group[[2]]
        costs_file_path <-
          file.path(model$outputdir,
                    paste0(taskNaics, "_g", taskGroup, ".costs.csv"))
        cost_file_exists <- file.exists(costs_file_path)
        write(print(
          paste0(
            Sys.time(),
            ": Finished ",
            asyncResults[["asyncTaskName"]],
            ", Running time: ",
            asyncResults[["elapsedTime"]],
            ", cost_file_exists: ",
            cost_file_exists
          )
        ),
        file = baseLogFilePath,
        append = TRUE)
        if (!cost_file_exists) {
          stop(paste(
            "***ERROR*** Did not find expected costs file '",
            costs_file_path,
            "'."
          ))
        }
      },
      debug = debugFuture
    ) #end call to startAsyncTask
    processRunningTasks(wait = FALSE, debug = TRUE)
  } #end loop over groups
  print(paste0(Sys.time(),": Finished naics: ", naics))
} #end for (naics_run_number in 1:nrow(naics_set))

#wait until all tasks are finished
processRunningTasks(wait = TRUE, debug=TRUE)

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
