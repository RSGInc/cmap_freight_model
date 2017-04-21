library(future)

#NOTE: the file that 'source's this should also call plan(multiprocess, workers=<desired number of workers>) for example:
#plan(multiprocess, workers=min(myNumTasks, MAX_PROCESSES))
#it is not required to specify workers -- if not then it will default to future::availableCores()

asyncTasksRunning <- list()

startAsyncTask <-
  function(asyncTaskName,
           futureObj,
           callback = NULL,
           debug = FALSE) {
    if (debug)
      print(paste0("startAsyncTask asyncTaskName '",
                   asyncTaskName,
                   "' called"))

    if (exists(asyncTaskName, asyncTasksRunning)) {
      stop(
        "Error: A task with the same asyncTaskName '",
        asyncTaskName,
        "' is already running. It is not known if it is running the same task"
      )
    }
    asyncTaskObject <- list(
      futureObj = futureObj,
      taskName = asyncTaskName,
      callback = callback,
      startTime = Sys.time()
    )
    asyncTasksRunning[[asyncTaskName]] <<- asyncTaskObject
  } #end startAsyncTask


getRunningTasksStatus <- function() {
  getRunningTaskStatus <- function(asyncTaskObject) {
    if (is.null(asyncTaskObject) ||
        length(names(asyncTaskObject)) < 4) {
      runningTaskStatus <- "[NULL]"
    } else {
      runningTaskStatus <-
        paste0(
          "[",
          asyncTaskObject[["taskName"]],
          "'s elapsed time: ",
          format(Sys.time() - asyncTaskObject[["startTime"]]),
          ", Finished?: ",
          resolved(asyncTaskObject[["futureObj"]]),
          "]"
        )
    }
    return(runningTaskStatus)
  }
  runningTasksStatus <-
    paste("# of running tasks: ",
          length(asyncTasksRunning),
          paste0(collapse = ", ", lapply(
            asyncTasksRunning, getRunningTaskStatus
          )))
  return(runningTasksStatus)
} #end getRunningTasksStatus

#' Meant to called periodically, this will check all running asyncTasks for completion
#' Returns number of remaining tasks so could be used as a boolean
processRunningTasks <-
  function(wait=FALSE, catchErrors = TRUE,
           debug = FALSE,
           maximumTasksToResolve = -1)
  {
    numTasksResolved <- 0
    for (asyncTaskName in names(asyncTasksRunning)) {
      if ((maximumTasksToResolve > 0) &&
          (numTasksResolved >= maximumTasksToResolve)) {
        if (debug)
          print(
            paste0(
              "processRunningTasks: stopping checking for resolved tasks because maximumTasksToResolve (",
              maximumTasksToResolve,
              ") already resolved."
            )
          )
        break
      } #end checking if need to break because of maximumTasksToResolve
      asyncTaskObject <- asyncTasksRunning[[asyncTaskName]]
      asyncFutureObject <- asyncTaskObject$futureObj
      if (!wait || resolved(asyncFutureObject)) {
        taskResult <- NULL
        numTasksResolved <- numTasksResolved + 1
        #NOTE future will send any errors it caught when we ask it for the value -- same as if we had evaluated the expression ourselves
        caughtError <- NULL
        caughtWarning <- NULL
        if (catchErrors) {
          withCallingHandlers(
            expr = {
              taskResult <- value(asyncFutureObject)
            },
            warning = function(w) {
              caughtWarning <- w
              print(
                paste0(
                  "***WARNING*** processRunningTasks: '",
                  asyncTaskName,
                  "' returned a warning: ",
                  w
                )
              )
              print(sys.calls())
            },
            error = function(e) {
              caughtError <- e
              print(
                paste0(
                  "***ERROR*** processRunningTasks: '",
                  asyncTaskName,
                  "' returned an error: ",
                  e
                )
              )
              print(sys.calls())
            }
          )#end withCallingHandlers
        } #end if catch errors
        else {
          #simply fetch the value -- if exceptions happened they will be thrown by the Future library when we call value and
          #therefore will propagate to the caller
          taskResult <- value(asyncFutureObject)
        }
        rm(asyncFutureObject)
        startTime <- asyncTaskObject$startTime
        endTime <- Sys.time()
        elapsedTime <- format(endTime - startTime)
        if (debug)
          print(
            paste0(
              "processRunningTasks finished: '",
              asyncTaskName,
              "'. startTime: ",
              startTime,
              ", endTime: ",
              endTime,
              "', elapsed time: ",
              elapsedTime
            )
          )
        callback <- asyncTasksRunning[[asyncTaskName]]$callback
        asyncTasksRunning[[asyncTaskName]] <<- NULL
        if (!is.null(callback)) {
          callback(
            list(
              asyncTaskName = asyncTaskName,
              taskResult = taskResult,
              startTime = startTime,
              endTime = endTime,
              elapsedTime = elapsedTime,
              caughtError = caughtError,
              caughtWarning = caughtWarning
            )
          )
        }
      } #end if resolved
    }#end loop over async data items being loaded
    #Any more asynchronous data items being loaded?
    return(length(asyncTasksRunning))
  } # end processRunningTasks

testAsync <- function(loops = future::availableCores() - 1) {
  fakeDataProcessing <- function(name, duration, sys_sleep = FALSE) {
    if (sys_sleep) {
      Sys.sleep(duration)
    } else {
      start_time <- Sys.time()
      elapsed_time <- -1
      repeat {
        elapsed_time = Sys.time() - start_time
        print(paste0(name, " elapsed time: ", elapsed_time))
        if (elapsed_time < duration) {
          Sys.sleep(1)
        } else {
          break
        }
      } #end repeat
    } #end else not using long sleep
    return(data.frame(name = name, test = Sys.time()))
  } #end fakeDataProcessing

  maxRunningTasks <- max(1, future::availableCores() - 1)
  sleepTime <- 2
  loops <- 10 #
  baseWait <- 3
  for (loopNumber in 1:loops) {
    repeat {
      numTasksRunning <-
        processRunningTasks(debug=TRUE)
      print(
        paste0(
          Sys.time(),
          ": Waiting for some of the ",
          numTasksRunning,
          " running tasks to finish so can work on remaining ",
          ((loops - loopNumber) + 1),
          " tasks. Tasks: ",
          getRunningTasksStatus()
        )
      )
      if (numTasksRunning < maxRunningTasks) {
        break
      } else {
        Sys.sleep(sleepTime)
      }
    } #end while waiting for free slots
    duration <- baseWait + loopNumber
    dataName <-
      paste0("FAKE_PROCESSED_DATA_testLoop-",
             loopNumber,
             "_duration-",
             duration)
    startAsyncTask(dataName,
                   futureObj = future(fakeDataProcessing(dataName, duration)),
                   debug = TRUE)
    print(paste0("loop: ", loopNumber, " ", getRunningTasksStatus()))
  }
  print(paste0(
    Sys.time(),
    ": After all tasks submitted: ",
    getRunningTasksStatus()
  ))

  #could call processRunningTasks(wait=TRUE, debug = TRUE) to have blocked...
  #but for this I will poll...
  while (processRunningTasks(debug = TRUE) > 0)
  {
    Sys.sleep(1)
    print(getRunningTasksStatus())
  }

  print(paste0(
    "At the end the status should have no running tasks: ",
    getRunningTasksStatus()
  ))
} #end testAsync

#testAsync()
testAsyncWithSink <- function() {
  sinkFile <- tempfile("asyncSinkTest_")
  print(paste0("sinkFile: ", sinkFile))
  log <- file(sinkFile, open = "wt")
  if (!file.exists(sinkFile)) {
    stop("expected temp file does not exist!")
  }
  print(paste0(
    "start sink.number(type = 'output'): ",
    sink.number(type = "output")
  ))
  print(paste0(
    "start sink.number(type = 'message'): ",
    sink.number(type = "message")
  ))
  sink(log, split = T)
  sink(log, type = "message")
  print(paste0(
    "after sink sink.number(type = 'output'): ",
    sink.number(type = "output")
  ))
  print(paste0(
    "after sink sink.number(type = 'message'): ",
    sink.number(type = "message")
  ))
  writeLines("Does calling writeLines when file is a sink cause problems?", log)
  testAsync()
  #remove sinks
  sink(type = "message")
  sink()
  print(paste0(
    "after unsink sink.number(type = 'output'): ",
    sink.number(type = "output")
  ))
  print(paste0(
    "after unsink sink.number(type = 'message'): ",
    sink.number(type = "message")
  ))
  cat(paste0(collapse = "\n", readLines(sinkFile)))
  close(log)
}
#testAsyncWithSink()
