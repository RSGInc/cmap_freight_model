#' Unfortunately getScriptDirectory cannot be moved to 00_utilities.R because until we actually have
#' the current script directory we don't know how to source any other file
#' because we want to use relative paths and they have to relative to something KNOWN

#' This simply adds a timestamp and prints a message to the console which is immediately flushed
debugConsole <- function(msg) {
  time <- paste(Sys.time())
  logLine <- paste0(time, ": ", msg)
  print(logLine)
  flush.console()
  return(logLine)
}

isPeterDevelopmentMode <-
  dir.exists(model$outputdir) &&
  (length(list.files(model$outputdir)) > 10) &&
  interactive() &&
  (Sys.info()[["user"]] == "peter.andrews")
