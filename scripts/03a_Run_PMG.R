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
naics <- args[1]
groups <- as.integer(args[2])
sprod <- as.integer(args[3])
basedir <- args[4]
outputdir <- args[5]

setwd(basedir)

#Start logging
PMG_Log <- file.path(outputdir, paste0(naics, "_PMG_Log.txt"))
log <- file(PMG_Log, open = "wt")
sink(log, split = T)
sink(log, type = "message")

#load the pmg workspace
load(file.path(outputdir, "PMG_Workspace.Rdata"))

#load the workspace for this naics code
load(file.path(outputdir, paste0(naics, ".Rdata")))


library(rFreight, lib.loc = "./library/")
loadPackage("data.table")
loadPackage("reshape")
loadPackage("reshape2")
loadPackage("bit64")
options(datatable.auto.index = FALSE)
##async tools
##########################################
useFuture <- TRUE
debugFuture <- TRUE
source("./scripts/00_Async_Tasks.R")
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
      log_file_path <-
        normalizePath(file.path(log_folder, paste0(naics_io_code,
                                                   "_g",
                                                   groupnum)))
      file.create(log_file_path)
    } else {
      log_file_path <- ""
    }

    if (writelog)
      write(print(
        paste(
          1,
          "runPMGLocal naics:",
          naics_io_code,
          "group:",
          groupnum,
          "log_file_path:",
          log_file_path
        )
      ), file = log_file_path, append = TRUE)


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
      ), file = log_file_path, append = TRUE)

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
      ), file = log_file_path, append = TRUE)

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
      ), file = log_file_path, append = TRUE)


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
      ), file = log_file_path, append = TRUE)

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
      ), file = log_file_path, append = TRUE)

    #
    #build system call:
    system2(
      pmgexe,
      args = paste("-i", inipath, "-p", ioprefix, "-d", inpath, "-o", outpath),
      stdout = log_file_path,
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
      ), file = log_file_path, append = TRUE)

  } #end runPMGLocal

#list to hold the summarized outputs, file to hold timings
pmgoutputs <- list()
pmgtimes <-
  file(file.path(outputdir, paste0(naics, ".txt")), open = "wt")
writeLines(print(paste(
  "Starting:", naics, "Current time", Sys.time()
)), con =
  pmgtimes)

processPMGOutputs <- function(g, log_file_path = NULL) {
  doOutputsExist <-
    file.exists(file.path(outputdir, paste0(naics, "_g", g, ".out.csv")))
  if (doOutputsExist) {
    if (!is.null(log_file_path))
      write(print(
        paste(
          "Processing Outputs:",
          naics,
          "Group",
          g,
          "Current time",
          Sys.time()
        )
      ), file = log_file_path, append = TRUE)

    pmgout <-
      fread(file.path(outputdir, paste0(naics, "_g", g, ".out.csv")))

    setnames(pmgout, c("BuyerId", "SellerId"), c("BuyerID", "SellerID"))

    #get just the results from the final iteration

    pmgout <- pmgout[Last.Iteration.Quantity > 0]

    #apply fix for bit64/data.table handling of large integers in rbindlist

    pmgout[, Quantity.Traded := as.character(Quantity.Traded)]

    pmgout[, Last.Iteration.Quantity := as.character(Last.Iteration.Quantity)]

    load(file.path(outputdir, paste0(naics, "_g", g, ".Rdata")))

    pmgoutputs[[g]] <<-
      merge(pc, pmgout, by = c("BuyerID", "SellerID"))

    rm(pmgout, pc)
  }
  return(doOutputsExist)
} #end processPMGOutputs

#loop over the groups and run the games, and process outputs
for (g in 1:groups) {
  if (!processPMGOutputs(g, pmgtimes)) {
    #output file does not exist so must be calculated
    #if all processors are in use wait until one is available
    repeat {
      numTasksRunning <-
        processRunningTasks(useFuture, debugFuture)
      if (debugFuture)
        print(
          paste0(
            Sys.time(),
            ": Waiting for some of the ",
            numTasksRunning,
            "PMG running tasks to finish so can work on remaining ",
            ((groups - g) + 1),
            " tasks. Tasks: ",
            getRunningTasksStatus()
          )
        )
      if (numTasksRunning < model$scenvars$maxrscriptinstances) {
        break
      } else {
        Sys.sleep(30)
      }
    } #end while waiting for free slots

    taskName <-       paste0("RunPMG_naics-",
                             naics,
                             "_group-",
                             g,
                             "_of_",
                             groups,
                             "_sprod-",
                             sprod)
    write(paste0(Sys.time(), ": Starting ", taskName),
          file = pmgtimes,
          append = TRUE)

    doAsync <- TRUE

    if (!doAsync) {
      runPMGLocal(
        naics,
        g,
        writelog = TRUE,
        #model$scenvars$pmglogging,
        wait = TRUE,
        inpath = outputdir,
        outpath = outputdir,
        log_folder = outputdir
      )
      doesOutputExist <- processPMGOutputs(g, pmgtimes)
      print(
        paste(
          "doesOutputExist?:",
          doesOutputExist,
          naics,
          "Group",
          g,
          "Current time",
          Sys.time()
        )
      )

    } else {
      startAsyncTask(
        taskName,
        future({
          #call to runPMG to run the game
          runPMGLocal(
            naics,
            g,
            writelog = TRUE,
            #model$scenvars$pmglogging,
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



          msg <- paste(Sys.time(),
                       "Completed:",
                       asyncResults[["asyncTaskName"]],
                       "Running time:",
                       asyncResults[["elapsedTime"]])
          print(msg)
          if (!model$scenvars$pmglogging) {
            writeLines(msg,
                       con = pmggrouptimes)
            close(pmggrouptimes)
          }

          writeLines(msg,
                     con = pmgtimes)

          print(paste(
            "Deleting Inputs:",
            naics,
            "Group",
            g,
            "Current time",
            Sys.time()
          ))

          file.remove(file.path(outputdir, paste0(naics, "_g", g, ".costs.csv")))
          file.remove(file.path(outputdir, paste0(naics, "_g", g, ".buy.csv")))
          file.remove(file.path(outputdir, paste0(naics, "_g", g, ".sell.csv")))

          doesOutputExist <- processPMGOutputs(g, pmgtimes)
          print(
            paste(
              "doesOutputExist?:",
              doesOutputExist,
              naics,
              "Group",
              g,
              "Current time",
              Sys.time()
            )
          )
        },
        #end callback
        debug = debugFuture
      ) #end call to startAsyncTask
    } #end else doAsync
  } #end if file does not exist already
}#end loop over groups

#loop until all running tasks are finished
repeat {
  numTasksRunning <-
    processRunningTasks(useFuture, debugFuture)
  if (debugFuture)
    print(
      paste0(
        Sys.time(),
        ": Waiting for final ",
        numTasksRunning,
        "PMG running tasks to finish. ",
        getRunningTasksStatus()
      )
    )
  if (numTasksRunning < model$scenvars$maxrscriptinstances) {
    break
  } else {
    Sys.sleep(30)
  }
} #end while waiting for all tasks to finish

if (debugFuture)
  print(paste0(Sys.time(),
               ": 1 run_PMG"))
#convert output list to one table, add to workspace, and save
#apply fix for bit64/data.table handling of large integers in rbindlist
pairs <- rbindlist(pmgoutputs)
if (debugFuture)
  print(paste0(
    Sys.time(),
    ": 2 run_PMG",
    "names(pairs):",
    paste0(collapse = ", ", names(pairs))
  ))
rm(pmgoutputs)
pairs[, Quantity.Traded := as.integer64.character(Quantity.Traded)]
if (debugFuture)
  print(paste0(Sys.time(),
               ": 3 run_PMG"))

pairs[, Last.Iteration.Quantity := as.integer64.character(Last.Iteration.Quantity)]
if (debugFuture)
  print(paste0(Sys.time(),
               ": 4 run_PMG"))
save(consc, prodc, pairs, file = file.path(outputdir, paste0(naics, ".Rdata")))
if (debugFuture)
  print(paste0(Sys.time(),
               ": 5 run_PMG"))

#close off logging
writeLines(print(
  paste(
    "Completed Processing Outputs:",
    naics,
    "Current time",
    Sys.time()
  )
),
con = pmgtimes)
close(pmgtimes)
if (debugFuture)
  print(paste0(Sys.time(),
               ":  run_PMG"))

#end sinking
sink(type = "message")
sink()
quit(save = "no", status = 0)
