##############################################################################################
#Title:             CMAP Agent Based Freight Forecasting Code
#Project:           CMAP Agent-based economics extension to the meso-scale freight model
#Description:       03a_RunPMG.R produces the inputs and runs the PMG games for a NAICS code
#Date:              June 26 28, 2014
#Author:            Resource Systems Group, Inc.
#Copyright:         Copyright 2014 RSG, Inc. - All rights reserved.
##############################################################################################

#-----------------------------------------------------------------------------------
#Step 3a Run PMG
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

library(rFreight, lib.loc = "./library/")
loadPackage("data.table")
loadPackage("reshape")
loadPackage("reshape2")
loadPackage("bit64")
options(datatable.auto.index = FALSE)
##async tools
##########################################
debugFuture <- TRUE
source("./scripts/FutureTaskProcessor.R")
#only allocate as many workers as we need (including one for future itself) or to the specified maximum
plan(multiprocess,
     workers = model$scenvars$maxrscriptinstances)
########################################

#load required packages
#library(rFreight, lib.loc = "./library/")
#' Builds the system call to the PMG application and runs the application
#'
#' Builds the systems call including the command line options to run the PMG
#' application for a particular NAICs market and group sample from within the
#' full set of buyers and sellers in that NAICS market.
#'
#' @param naics_io_code BEA io code for the commodity to be run, i.e., that matches with the filenaming used for the buy/sell/costs files (character string).
#' @param groupnum is the sample group numbers for the group to be run, i.e., that matches with the numbering used for the buy/sell/costs files (integer).
#' @param writelog TRUE/FALSE to indicate whether to capture standard output from the PMG application is a text file.
#' @param invisible TRUE/FALSE to indicate whether to show the command window or not.
#' @param wait TRUE/FALSE to indicate whether to R should wait for the PMG application to finish, or (if false) should run the PMG application asynchronously.
#' @param pmgexe Path to the pmg executable, defaults to "./PMG/pmg.exe"
#' @param inipath Path to the ini file, defaults to "./PMG/PMG.ini"
#' @param inpath Path to the PMG inputs folder, defaults to "./outputs"
#' @param outpath Path to the PMG outputs folder, defaults to "./outputs"
#' @param log_folder Path to the log file folder, defaults to "./outputs"
#' @keywords PMG
#' @export
#' @examples
#' \dontrun{
#' runPMG(naics,g,writelog=FALSE,wait=TRUE)
#' }

runPMGLocal <-
  function(naics_io_code,
           groupnum = NA,
           writelog = FALSE,
           invisible = TRUE,
           wait = FALSE,
           pmgexe = "./PMG/pmg.exe",
           inipath = "./PMG/pmg.ini",
           inpath = "./outputs",
           outpath = "./outputs",
           log_folder = "./outputs") {
    if (writelog)
      print(paste("log_folder:", log_folder))

    if (writelog) {
      pmg_log_file_path <-
        normalizePath(file.path(log_folder, paste0(naics_io_code,
                                                   "_g",
                                                   groupnum, ".txt")))
      file.create(pmg_log_file_path)
    } else {
      pmg_log_file_path <- ""
    }

    if (writelog)
      write(print(
        paste(
          1,
          "runPMGLocal naics:",
          naics_io_code,
          "group:",
          groupnum,
          "pmg_log_file_path:",
          pmg_log_file_path
        )
      ), file = pmg_log_file_path, append = TRUE)


    #location of PMG executable: in the PMG folder, called PMG.exe
    #pmgexe <- gsub("/", "\\", pmgexe, fixed = TRUE)
    pmgexe <- normalizePath(pmgexe)
    if (writelog)
      write(print(
        paste(
          2,
          "runPMGLocal naics:",
          naics_io_code,
          "group:",
          groupnum,
          "pmgexe: ",
          pmgexe
        )
      ), file = pmg_log_file_path, append = TRUE)

    #command line options
    # 1.  Specify ini file path:
    #     -i C:\path\to\file\pmg.ini
    #inipath <- gsub("/", "\\", inipath, fixed = TRUE)
    inipath <- normalizePath(inipath)

    if (writelog)
      write(print(
        paste(
          3,
          "runPMGLocal naics:",
          naics_io_code,
          "group:",
          groupnum,
          "inipath:",
          inipath
        )
      ), file = pmg_log_file_path, append = TRUE)

    # 2. specify data input and output file name prefixes
    # -p naics_io_code
    ioprefix <- naics_io_code
    if (!is.na(groupnum))
      ioprefix <- paste0(naics_io_code, "_g", groupnum)
    if (writelog)
      write(print(
        paste(
          4,
          "runPMGLocal naics:",
          naics_io_code,
          "group:",
          groupnum,
          "ioprefix:",
          ioprefix
        )
      ), file = pmg_log_file_path, append = TRUE)


    # 3. specify data directory path for input files files
    # location of naics_io_code.buy.csv, naics_io_code.sell.csv and  naics_io_code.costs.csv
    # -d C:\path\to\inputs
    #inpath <- gsub("/", "\\", inpath, fixed = TRUE)
    inpath <- normalizePath(inpath)
    if (writelog)
      write(print(
        paste(
          5,
          "runPMGLocal naics:",
          naics_io_code,
          "group:",
          groupnum,
          "inpath:",
          inpath
        )
      ), file = pmg_log_file_path, append = TRUE)

    # 4. specify directory path for output file
    # locations of naics_io_code.out.csv
    # -o C:\path\to\outputs
    #outpath <- gsub("/", "\\", outpath, fixed = TRUE)
    outpath <- normalizePath(outpath)

    if (writelog)
      write(print(
        paste(
          6,
          "runPMGLocal naics:",
          naics_io_code,
          "group:",
          groupnum,
          "outpath:",
          outpath
        )
      ), file = pmg_log_file_path, append = TRUE)

    #
    #build system call:
    system2(
      pmgexe,
      args = paste("-i", inipath, "-p", ioprefix, "-d", inpath, "-o", outpath),
      stdout = pmg_log_file_path,
      invisible = invisible,
      wait = wait
    )
    if (writelog)
      write(print(
        paste(
          7,
          "runPMGLocal AFTER CALL TO pmgexe '",
          pmgexe,
          "' naics:",
          naics_io_code,
          "group:",
          groupnum
        )
      ), file = pmg_log_file_path, append = TRUE)

  } #end runPMGLocal

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
    file.path(outputdir, paste0(naics, "_PMGRun_Log.txt"))
  file.create(log_file_path)

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

  naicsInProcess[[paste0("naics-",naics)]] <- list() #create place to accumulate group results

  #loop over the groups and run the games, and process outputs
  for (g in 1:groups) {
    taskName <-       paste0("RunPMG_naics-",
                             naics,
                             "_group-",
                             g,
                             "_of_",
                             groups,
                             "_sprod-",
                             sprod)
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
        #call to runPMG to run the game
        runPMGLocal(
          naics,
          g,
          writelog = model$scenvars$pmglogging,
          wait = TRUE,
          inpath = outputdir,
          outpath = outputdir,
          log_folder = outputdir
        )
      }),
      callback = function(asyncResults) {
        # asyncResults is: list(asyncTaskName,
        #                        taskResult,
        #                        startTime,
        #                        endTime,
        #                        elapsedTime,
        #                        caughtError,
        #                        caughtWarning)

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

                write(print(
          paste0(
            Sys.time(),
            ": task '",
            taskName,
            "' finished. Elapsed time since submitted: ",
            asyncResults[["elapsedTime"]]
          )
        ), file = task_log_file_path, append = TRUE)

        expectedOutputFile <-
          file.path(outputdir, paste0(taskInfo$taskNaics, "_g", taskInfo$taskGroup, ".out.csv"))
        if (!file.exists(expectedOutputFile)) {
          msg <- paste0(
            Sys.time(),
            ": Expected PMG output file '",
            expectedOutputFile,
            "' does not exist!"
          )
          write(print(msg), file = task_log_file_path, append = TRUE)
          stop(msg)
        }
        pmgout <-
          fread(expectedOutputFile)

        setnames(pmgout,
                 c("BuyerId", "SellerId"),
                 c("BuyerID", "SellerID"))

        #get just the results from the final iteration

        pmgout <- pmgout[Last.Iteration.Quantity > 0]

        #apply fix for bit64/data.table handling of large integers in rbindlist

        pmgout[, Quantity.Traded := as.character(Quantity.Traded)]

        pmgout[, Last.Iteration.Quantity := as.character(Last.Iteration.Quantity)]

        load(file.path(outputdir, paste0(taskInfo$taskNaics, "_g", taskInfo$taskGroup, ".Rdata")))

        naicsKey <- paste0("naics-",taskInfo$taskNaics)
        groupoutputs <- naicsInProcess[[naicsKey]]
        groupKey <- paste0("group-",taskInfo$taskGroup)
        groupoutputs[[groupKey]] <-
          merge(pc, pmgout, by = c("BuyerID", "SellerID"))

        #don't understand why this is necessary but apparently have to re-store list
        naicsInProcess[[naicsKey]] <<- groupoutputs

        rm(pmgout, pc)
        write(print(paste0(Sys.time(),
                     ": Deleting Inputs: ",
                     taskInfo$taskNaics,
                     " Group: ",
                     taskInfo$taskGroup)), file=task_log_file_path, append=TRUE)

        file.remove(file.path(outputdir, paste0(taskInfo$taskNaics, "_g", taskInfo$taskGroup, ".costs.csv")))
        file.remove(file.path(outputdir, paste0(taskInfo$taskNaics, "_g", taskInfo$taskGroup, ".buy.csv")))
        file.remove(file.path(outputdir, paste0(taskInfo$taskNaics, "_g", taskInfo$taskGroup, ".sell.csv")))

        if (length(groupoutputs) == taskInfo$taskGroups) {
          #convert output list to one table, add to workspace, and save
          #apply fix for bit64/data.table handling of large integers in rbindlist
          naicsRDataFile <-
            file.path(outputdir, paste0(taskInfo$taskNaics, ".Rdata"))
          load(naicsRDataFile)
          write(print(
            paste0("loaded '", naicsRDataFile, "' nrow(consc)=", nrow(consc), " nrow(prodc)=", nrow(prodc))))
          pairs <- rbindlist(groupoutputs)
          pairs[, Quantity.Traded := as.integer64.character(Quantity.Traded)]

          pairs[, Last.Iteration.Quantity := as.integer64.character(Last.Iteration.Quantity)]
          write(print(
            paste0("loaded '", naicsRDataFile, "' nrow(consc)=", nrow(consc), " nrow(prodc)=", nrow(prodc))))
          save(consc, prodc, pairs, file = naicsRDataFile)
          rm(consc, prodc, pairs)

          #delete naic from tracked outputs
          naicsInProcess[[naicsKey]] <<- NULL

          write(print(
            paste0(
              Sys.time(),
              ": Completed Processing Outputs of all ",
              taskInfo$taskGroups,
              " groups for naics ",
              taskInfo$taskNaics,
              ". Remaining naicsInProcess=",
              paste0(collapse=", ", names(naicsInProcess))
            )
          ), file = task_log_file_path, append = TRUE)
        } #end if all groups in naic are finished
      },
      #end callback
      debug = FALSE
    ) #end call to startAsyncTask
    processRunningTasks(wait = FALSE, debug = TRUE, maximumTasksToResolve=1)
  }#end loop over groups
} #end for (naics_run_number in 1:nrow(naics_set))

#wait until all tasks are finished
processRunningTasks(wait = TRUE, debug = TRUE)

if (length(naicsInProcess) != 0) {
  stop(paste(
    "At end of Run_PMG there were still some unfinished naics! Unfinished: ",
    paste0(collapse = ", ", names(naicsInProcess))
  ))
}

quit(save = "no", status = 0)
