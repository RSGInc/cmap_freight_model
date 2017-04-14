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
  function(asyncDataName, futureObj, callback = NULL, debug=FALSE) {
    if (debug) debugConsole(paste0(
      "startAsyncTask asyncDataName '",
      asyncDataName,
      "' called"
    ))
    asyncTasksRunning[[asyncDataName]] <<-
      list(futureObj = futureObj,
           callback = callback)
  } #end startAsyncTask


#' Meant to called periodically, this will check all running asyncTasks for completion
#' Returns number of remaining tasks so could be used as a boolean
checkAsyncTasksRunning <- function(catchErrors = FALSE, debug=FALSE)
{
  for (asyncDataName in names(asyncTasksRunning)) {
    asyncObject <- asyncTasksRunning[[asyncDataName]]$futureObj
    if (resolved(asyncObject)) {
      if (debug) debugConsole(paste0(
        "checkAsyncTasksRunning resolved: '",
        asyncDataName,
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
              asyncDataName,
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
              asyncDataName,
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
      callback <- asyncTasksRunning[[asyncDataName]]$callback
      asyncTasksRunning[[asyncDataName]] <<- NULL
      if (!is.null(callback)) {
        callback(asyncDataName, asyncResult, caughtError, caughtWarning)
      }
    } #end if resolved
  }#end loop over async data items being loaded
  #Any more asynchronous data items being loaded?
  return(length(asyncTasksRunning))
} # end checkAsyncTasksRunning
