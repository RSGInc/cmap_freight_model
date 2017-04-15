##############################################################################################
#Title:             CMAP Agent Based Freight Forecasting Code
#Project:           CMAP Agent-based economics extension to the meso-scale freight model
#Description:       00_asyncTasks provides startAsyncTask and checkAsyncTasksRunning to
# provide an easy way to run asynchronous
#Date:              April 14, 2017
#Author:            Resource Systems Group, Inc.
#Copyright:         Copyright 2017 RSG, Inc. - All rights reserved.
##############################################################################################
library(future)
plan(multiprocess)

source("./scripts/00_utilities.R")

asyncTasksRunning <- list()

startAsyncTask <-
  function(asyncTaskName, futureObj, callback = NULL, debug=FALSE) {
    if (debug) debugConsole(paste0(
      "startAsyncTask asyncTaskName '",
      asyncTaskName,
      "' called"
    ))

    if(exists(asyncTaskName, asyncTasksRunning)) {
      stop("Error: A task with the same asyncTaskName '", asyncTaskName, "' is already running. It is not known if it is running the same task")
    }
    asyncTasksRunning[[asyncTaskName]] <<-
      list(futureObj = futureObj,
           callback = callback)
  } #end startAsyncTask


#' Meant to called periodically, this will check all running asyncTasks for completion
#' Returns number of remaining tasks so could be used as a boolean
checkAsyncTasksRunning <- function(catchErrors = FALSE, debug=FALSE, maximumTasksToResolve = -1)
{
  numTasksResolved <- 0
  for (asyncTaskName in names(asyncTasksRunning)) {
    if ((maximumTasksToResolve > 0) && (numTasksResolved >= maximumTasksToResolve)) {
      if (debug) debugConsole(
        paste0(
          "checkAsyncTasksRunning: stopping checking for resolved tasks because maximumTasksToResolve (", maximumTasksToResolve, ") already resolved."
        )
      )
      break
    } #end checking if need to break because of maximumTasksToResolve
    asyncObject <- asyncTasksRunning[[asyncTaskName]]$futureObj
    if (resolved(asyncObject)) {
      numTasksResolved <- numTasksResolved + 1
      if (debug) debugConsole(paste0(
        "checkAsyncTasksRunning resolved: '",
        asyncTaskName,
        "'"
      ))
      #NOTE future will send any errors it caught when we ask it for the value -- same as if we had evaluated the expression ourselves
      caughtError <- NULL
      caughtWarning <- NULL
      if (catchErrors) {
      tryCatch(
        expr = {
          asyncResult <- value(asyncObject)
        },
        warning = function(w) {
          caughtWarning <- w
          debugConsole(
            paste0(
              "checkAsyncTasksRunning: '",
              asyncTaskName,
              "' returned a warning: ",
              w
            )
          )
        },
        error = function(e) {
          caughtError <- e
          debugConsole(
            paste0(
              "checkAsyncTasksRunning: '",
              asyncTaskName,
              "' returned an error: ",
              e
            )
          )
        }
      )#end tryCatch
      } #end if catch errors
      else {
        #simply fetch the value -- if exceptions happened they will be thrown by the Future library when we call value and
        #therefore will propagate to the caller
        asyncResult <<- value(asyncObject)
      }
      callback <- asyncTasksRunning[[asyncTaskName]]$callback
      asyncTasksRunning[[asyncTaskName]] <<- NULL
      if (!is.null(callback)) {
        callback(asyncTaskName, asyncResult, caughtError, caughtWarning)
      }
    } #end if resolved
  }#end loop over async data items being loaded
  #Any more asynchronous data items being loaded?
  return(length(asyncTasksRunning))
} # end checkAsyncTasksRunning
