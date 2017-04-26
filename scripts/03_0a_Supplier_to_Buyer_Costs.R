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
#load the pmg workspace
load(file.path(outputdir, "PMG_Workspace.Rdata"))

#https://github.com/tdhock/namedCapture
if (!require(namedCapture)) {
  if (!require(devtools)) {
    install.packages("devtools")
  }
  devtools::install_github("tdhock/namedCapture")
}
library(namedCapture)

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
     workers = model$scenvars$maxcostrscripts)
########################################

#list to hold the group outputs for any currently running naics
naicsInProcess <- list()

load(file.path(outputdir, "naics_set.Rdata"))
naics_set <-
  subset(naics_set, NAICS %in% model$scenvars$pmgnaicstorun)

if (nrow(naics_set) != length(model$scenvars$pmgnaicstorun)) {
  stop(
    paste(
      "Some of model$scenvars$pmgnaicstorun were not found in the naics_set. Number requested=",
      length(model$scenvars$pmgnaicstorun),
      ", number found=",
      nrow(naics_set)
    )
  )
}


for (naics_run_number in 1:nrow(naics_set)) {
  naics <- naics_set$NAICS[naics_run_number]
  groups <- naics_set$groups[naics_run_number]
  sprod <- ifelse(naics_set$Split_Prod[naics_run_number], 1, 0)

  log_file_path <-
    file.path(outputdir, paste0(naics, "_PMGSetup_Log.txt"))
  file.create(log_file_path)

  naicsInProcess[[paste0("naics-",naics)]] <- list() #create place to accumulate group results

  write(print(
    paste0(
      Sys.time(),
      ": Starting naics: ",
      naics,
      " with ",
      groups,
      " groups. sprod: ",
      sprod
    )
  ),
  file = log_file_path,
  append = TRUE)

  #load the workspace for this naics code
  load(file.path(outputdir, paste0(naics, ".Rdata")))

  #check whether sampling within the group has been done and if not run that function
  if (!"group" %in% names(prodc))
    create_pmg_sample_groups(naics, groups, sprod)


  #loop over the groups and prepare the files for running the games
  for (g in 1:groups) {

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
    write(print(
      paste0(
        Sys.time(),
        ": Submitting task '",
        taskName,
        "' to join ",
        getNumberOfRunningTasks(), " currently running tasks"
      )
    ), file = log_file_path, append = TRUE)
    startAsyncTask(
      taskName,
      future({
        ## Heither, revised 10-05-2015: File Cleanup if Outputs folder being re-used from previous run
        ## -- Delete NAICS_gX.sell file if exists from prior run (existence will prevent create_pmg_inputs from running)
        if (file.exists(file.path(outputdir, paste0(naics, "_g", g, ".sell.csv")))) {
          file.remove(file.path(outputdir, paste0(naics, "_g", g, ".sell.csv")))
        }

        if (!file.exists(file.path(outputdir, paste0(naics, "_g", g, ".sell.csv")))) {
          msg <-
            write(print(
              paste0(
                Sys.time(),
                " Starting creating inputs for: ",
                naics,
                ",  Group: ",
                g
              )
            ), file = log_file_path, append = TRUE)

          recycle_check_file_path <-
            file.path(
              outputdir,
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
        taskInfo <-
          data.table::data.table(
            namedCapture::str_match_named(
              taskName,
              "^.*naics[-](?<taskNaics>[^_]+)_group-(?<taskGroup>[^_]+)_of_(?<taskGroups>[^_]+)_sprod-(?<sprod>.*)$"
            )
          )[1, ]
        task_log_file_path <-
          file.path(outputdir, paste0(taskInfo$taskNaics, "_PMGRun_Log.txt"))

        naicsKey <- paste0("naics-",taskInfo$taskNaics)
        groupoutputs <- naicsInProcess[[naicsKey]]
        if (is.null(groupoutputs)) {
          stop(paste0("for taskInfo$taskNaics ", taskInfo$taskNaics, " naicsInProcess[[taskInfo$taskNaics]] (groupoutputs) is NULL! "))
        }

        groupKey <- paste0("group-",taskInfo$taskGroup)
        groupoutputs[[groupKey]] <-
          paste0(Sys.time(), ": Finished!")

        #don't understand why this is necessary but apparently have to re-store list
        naicsInProcess[[naicsKey]] <<- groupoutputs

        costs_file_path <-
          file.path(outputdir,
                    paste0(taskInfo$taskNaics, "_g", taskInfo$taskGroup, ".costs.csv"))
        cost_file_exists <- file.exists(costs_file_path)
        write(print(
          paste0(
            Sys.time(),
            ": Finished ",
            taskName,
            ", Elapsed time since submitted: ",
            asyncResults[["elapsedTime"]],
            ", cost_file_exists: ",
            cost_file_exists,
            " # of group results so far for this naics=", length(groupoutputs)
          )
        ),
        file = task_log_file_path,
        append = TRUE)
        if (!cost_file_exists) {
          msg <- paste("***ERROR*** Did not find expected costs file '",
                       costs_file_path,
                       "'.")
          write(print(msg), file = task_log_file_path, append = TRUE)
          stop(msg)
        }
        if (length(groupoutputs) == taskInfo$taskGroups) {
          #delete naic from tracked outputs
          naicsInProcess[[naicsKey]] <<- NULL
          write(print(
            paste0(
              Sys.time(),
              ": Completed Processing Outputs of all ",
              taskInfo$taskGroups,
              " groups for naics ",
              taskInfo$taskNaics,
              ". Remaining naicsInProcess=", paste0(collapse=", ", names(naicsInProcess))
            )
          ), file = task_log_file_path, append = TRUE)
        } #end if all groups in naic are finished
      },
      debug = FALSE
    ) #end call to startAsyncTask
    processRunningTasks(wait = FALSE, debug = TRUE, maximumTasksToResolve=1)
  } #end loop over groups
} #end for (naics_run_number in 1:nrow(naics_set))

#wait until all tasks are finished
processRunningTasks(wait = TRUE, debug = TRUE)

if (length(naicsInProcess) != 0) {
  stop(paste(
    "At end of 03_0a_Supplier_to_Buyer_Costs.R there were still some unfinished naics! Unfinished: ",
    paste0(collapse = ", ", names(naicsInProcess))
  ))
}

quit(save = "no", status = 0) #set status
